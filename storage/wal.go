package storage

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	googleproto "google.golang.org/protobuf/proto"
	"raft/proto"
)

type WAL struct {
	directory           string
	snapshotsDir        string
	currentSegment      *os.File
	lock                sync.Mutex
	PrevLogTerm         uint64
	CurrSequenceNo      uint64
	lastSnapshotIndex   uint64
	lastSnapshotPath    string
	bufWriter           *bufio.Writer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int64
	currentSegmentIndex int64
}

var (
	ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
	FileNotFound        = errors.New("log file not found")
	ErrCorruptedEntry   = errors.New("CRC mismatch, data might be corrupted")
	ErrCompacted        = errors.New("requested entries have been compacted into snapshot")
)

const (
	segmentPrefix     = "segment-"
	SnapshotThreshold = uint64(10000)
)

func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int64) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	snapshotsDir := filepath.Join(directory, "snapshots")
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int64
	if len(files) > 0 {
		lastSegmentID, err = findLastSegmentIndexinFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		file, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		lastSegmentID = 0
	}

	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}

	wal := &WAL{
		directory:           directory,
		snapshotsDir:        snapshotsDir,
		currentSegment:      file,
		CurrSequenceNo:      0,
		lastSnapshotIndex:   0,
		bufWriter:           bufio.NewWriter(file),
		shouldFsync:         enableFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentID,
	}

	if lastSnapIdx, lastSnapTerm, err := wal.getLatestSnapshotMetadata(); err == nil && lastSnapIdx > 0 {
		wal.lastSnapshotIndex = lastSnapIdx
		wal.PrevLogTerm = lastSnapTerm
		wal.CurrSequenceNo = lastSnapIdx
	}

	lastSeq, err := wal.getLastSequenceNo()
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	if lastSeq > wal.CurrSequenceNo {
		wal.CurrSequenceNo = lastSeq
	}

	if lastEntry, err := wal.getLastEntryInFile(file); err == nil && lastEntry != nil {
		wal.PrevLogTerm = lastEntry.Term
	}

	return wal, nil
}

func findLastSegmentIndexinFiles(files []string) (int64, error) {
	var lastSegmentId int64 = -1
	for _, file := range files {
		fileName := filepath.Base(file)
		if !strings.HasPrefix(fileName, segmentPrefix) || strings.HasSuffix(fileName, ".tmp") {
			continue
		}
		segmentId, err := strconv.ParseInt(strings.TrimPrefix(fileName, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}
	if lastSegmentId == -1 {
		return 0, nil
	}
	return lastSegmentId, nil
}

func createSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func calculateCRC(val []byte, seq uint64) uint32 {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, seq)
	return crc32.ChecksumIEEE(append(val, buf...))
}

func verifyCRC(entry *proto.RaftLog) bool {
	return entry.CRC == calculateCRC(entry.GetValue(), entry.GetIndex())
}

func (w *WAL) WriteEntry(cmd proto.Command, key string, val []byte, term uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.rotateIfNeeded(); err != nil {
		return err
	}

	nextSeq := w.CurrSequenceNo + 1

	entry := &proto.RaftLog{
		Index:   nextSeq,
		Term:    term,
		Key:     key,
		Value:   val,
		Command: cmd,
		CRC:     calculateCRC(val, nextSeq),
	}

	data := proto.MustMarshal(entry)

	if err := binary.Write(w.bufWriter, binary.LittleEndian, int64(len(data))); err != nil {
		return err
	}
	if _, err := w.bufWriter.Write(data); err != nil {
		return err
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	if w.shouldFsync {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}
	w.CurrSequenceNo = nextSeq
	w.PrevLogTerm = term

	if (w.CurrSequenceNo - w.lastSnapshotIndex) >= SnapshotThreshold {
		if err := w.createSnapshotLocked(); err != nil {
			return fmt.Errorf("creating snapshot: %w", err)
		}
	}

	return nil
}

func (w *WAL) CreateSnapshot() (string, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.createSnapshotFileLocked()
}

func (w *WAL) createSnapshotLocked() error {
	_, err := w.createSnapshotFileLocked()
	return err
}

func (w *WAL) createSnapshotFileLocked() (string, error) {
	if err := w.bufWriter.Flush(); err != nil {
		return "", err
	}
	if w.shouldFsync {
		_ = w.currentSegment.Sync()
	}

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", errors.New("no entries to snapshot")
	}

	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.GetIndex()
	lastTerm := lastEntry.GetTerm()

	if err := os.MkdirAll(w.snapshotsDir, 0755); err != nil {
		return "", err
	}

	kvMap := make(map[string][]byte)
	for _, entry := range entries {
		if entry.GetIndex() > lastIndex {
			break
		}
		switch entry.GetCommand() {
		case proto.Command_PUT:
			kvMap[entry.GetKey()] = entry.GetValue()
		case proto.Command_DELETE:
			delete(kvMap, entry.GetKey())
		}
	}

	targetFileName := fmt.Sprintf("snapshot-%d", lastIndex)
	targetFilePath := filepath.Join(w.snapshotsDir, targetFileName)
	tempFilePath := filepath.Join(w.snapshotsDir, fmt.Sprintf("snapshot-%d.tmp", lastIndex))

	tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}

	cleanup := func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFilePath)
	}

	if err := binary.Write(tempFile, binary.LittleEndian, lastIndex); err != nil {
		cleanup()
		return "", err
	}
	if err := binary.Write(tempFile, binary.LittleEndian, lastTerm); err != nil {
		cleanup()
		return "", err
	}

	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		val := kvMap[k]
		strVal := string(val)
		snapEntry := &proto.KVPair{
			Key:     k,
			Value:   &strVal,
			Command: proto.Command_PUT,
		}
		data := proto.MustMarshal(snapEntry)
		if err := binary.Write(tempFile, binary.LittleEndian, int64(len(data))); err != nil {
			cleanup()
			return "", err
		}
		if _, err := tempFile.Write(data); err != nil {
			cleanup()
			return "", err
		}
	}

	if err := tempFile.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempFilePath)
		return "", err
	}

	_ = os.Remove(targetFilePath)
	if err := os.Rename(tempFilePath, targetFilePath); err != nil {
		_ = os.Remove(tempFilePath)
		return "", fmt.Errorf("committing snapshot file: %w", err)
	}

	w.lastSnapshotIndex = lastIndex
	w.lastSnapshotPath = targetFilePath
	return targetFilePath, nil
}

func (w *WAL) getLatestSnapshotMetadata() (uint64, uint64, error) {
	files, err := filepath.Glob(filepath.Join(w.snapshotsDir, "snapshot-*"))
	if err != nil || len(files) == 0 {
		return 0, 0, nil
	}

	var maxIndex int64 = -1
	var latestFile string
	for _, f := range files {
		base := filepath.Base(f)
		if strings.HasSuffix(base, ".tmp") || !strings.HasPrefix(base, "snapshot-") {
			continue
		}
		idxStr := strings.TrimPrefix(base, "snapshot-")
		idx, err := strconv.ParseInt(idxStr, 10, 64)
		if err != nil {
			continue
		}
		if idx > maxIndex {
			maxIndex = idx
			latestFile = f
		}
	}

	if latestFile == "" {
		return 0, 0, nil
	}
	w.lastSnapshotPath = latestFile

	file, err := os.OpenFile(latestFile, os.O_RDONLY, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	var lastIndex, lastTerm uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		return 0, 0, err
	}
	if err := binary.Read(file, binary.LittleEndian, &lastTerm); err != nil {
		return 0, 0, err
	}
	return lastIndex, lastTerm, nil
}

func (w *WAL) rotateIfNeeded() error {
	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(w.bufWriter.Buffered()) >= w.maxFileSize {
		if err := w.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) rotateLog() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if w.shouldFsync {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}

	if err := w.currentSegment.Close(); err != nil {
		return err
	}

	w.currentSegmentIndex++
	newFile, err := createSegmentFile(w.directory, int(w.currentSegmentIndex))
	if err != nil {
		return err
	}
	w.currentSegment = newFile
	w.bufWriter = bufio.NewWriter(newFile)

	return nil
}

func (w *WAL) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.bufWriter.Flush(); err != nil {
		_ = w.currentSegment.Close()
		return err
	}
	if w.shouldFsync {
		_ = w.currentSegment.Sync()
	}
	return w.currentSegment.Close()
}

func (wal *WAL) getLastLogEntryLocked() (*proto.RaftLog, error) {
	if wal.bufWriter != nil {
		_ = wal.bufWriter.Flush()
	}

	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil || len(files) == 0 {
		return nil, nil
	}

	type segInfo struct {
		id   int64
		path string
	}
	var segs []segInfo
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasPrefix(base, segmentPrefix) || strings.HasSuffix(base, ".tmp") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(base, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, segInfo{id: id, path: f})
	}

	slices.SortFunc(segs, func(a, b segInfo) int {
		return cmp.Compare(b.id, a.id) // descending order to find last entry from newest segment
	})

	for _, seg := range segs {
		file, err := os.OpenFile(seg.path, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}
		entry, err := wal.getLastEntryInFile(file)
		_ = file.Close()
		if err == nil && entry != nil {
			return entry, nil
		}
	}

	return nil, nil
}

func (wal *WAL) getFirstLogEntryLocked() (*proto.RaftLog, error) {
	if wal.bufWriter != nil {
		_ = wal.bufWriter.Flush()
	}

	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil || len(files) == 0 {
		return nil, nil
	}

	type segInfo struct {
		id   int64
		path string
	}
	var segs []segInfo
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasPrefix(base, segmentPrefix) || strings.HasSuffix(base, ".tmp") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(base, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, segInfo{id: id, path: f})
	}

	slices.SortFunc(segs, func(a, b segInfo) int {
		return cmp.Compare(a.id, b.id) // ascending order to find first entry from oldest segment
	})

	for _, seg := range segs {
		file, err := os.OpenFile(seg.path, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}
		entry, err := wal.getFirstEntryInFile(file)
		_ = file.Close()
		if err == nil && entry != nil {
			return entry, nil
		}
	}

	return nil, nil
}

func (wal *WAL) getLastSequenceNo() (uint64, error) {
	entry, err := wal.getLastLogEntryLocked()
	if err != nil || entry == nil {
		return 0, err
	}
	return entry.GetIndex(), nil
}

func (wal *WAL) getFirstEntryInFile(file *os.File) (*proto.RaftLog, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var size int64
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if size <= 0 {
		return nil, nil
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	return unmarshalAndVerify(data)
}

func (wal *WAL) getLastEntryInFile(file *os.File) (*proto.RaftLog, error) {
	var previousSize int64
	var offset int64

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				if offset == 0 && previousSize == 0 {
					return nil, nil
				}

				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				return unmarshalAndVerify(data)
			}
			return nil, err
		}

		curOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		offset = curOffset
		previousSize = size

		if _, err := file.Seek(size, io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}

func (w *WAL) ReadAllEntries(file *os.File) ([]*proto.RaftLog, error) {
	var entries []*proto.RaftLog
	var size int64

	for {
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return entries, fmt.Errorf("unexpected end of file reading entry")
			}
			return entries, err
		}

		entry, err := unmarshalAndVerify(data)
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func unmarshalAndVerify(data []byte) (*proto.RaftLog, error) {
	var entry proto.RaftLog
	if err := googleproto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}

	if !verifyCRC(&entry) {
		return nil, ErrCorruptedEntry
	}
	return &entry, nil
}

func (w *WAL) ReadAll() ([]*proto.RaftLog, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.readAllFromOffsetLocked(-1)
}

func (w *WAL) readAllFromOffsetLocked(offset int) ([]*proto.RaftLog, error) {
	_ = w.bufWriter.Flush()

	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	type segInfo struct {
		id   int64
		path string
	}

	var segments []segInfo
	for _, file := range files {
		fileName := filepath.Base(file)
		if !strings.HasPrefix(fileName, segmentPrefix) || strings.HasSuffix(fileName, ".tmp") {
			continue
		}
		segmentIndex, err := strconv.ParseInt(strings.TrimPrefix(fileName, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		if segmentIndex < int64(offset) {
			continue
		}
		segments = append(segments, segInfo{id: segmentIndex, path: file})
	}

	slices.SortFunc(segments, func(a, b segInfo) int {
		return cmp.Compare(a.id, b.id)
	})

	var entries []*proto.RaftLog
	for _, seg := range segments {
		file, err := os.OpenFile(seg.path, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		entriesFromSegment, err := w.ReadAllEntries(file)
		_ = file.Close()
		if err != nil {
			return entries, err
		}

		entries = append(entries, entriesFromSegment...)
	}

	return entries, nil
}

func (wal *WAL) Repair(idx uint64) ([]*proto.RaftLog, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	_ = wal.bufWriter.Flush()

	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int64
	if len(files) > 0 {
		lastSegmentID, err = findLastSegmentIndexinFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("no log segments found, nothing to repair")
	}

	filePath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	var entries []*proto.RaftLog
	needsRepair := false

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			needsRepair = true
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			needsRepair = true
			break
		}

		entry, err := unmarshalAndVerify(data)
		if err != nil {
			needsRepair = true
			break
		}

		if entry.Index == idx {
			needsRepair = true
			break
		}
		entries = append(entries, entry)
	}
	_ = file.Close()

	if needsRepair {
		if err := wal.replaceWithFixedFile(filePath, entries); err != nil {
			return entries, err
		}
	}

	return entries, nil
}

func (wal *WAL) replaceWithFixedFile(targetFilePath string, entries []*proto.RaftLog) error {
	tempFilePath := fmt.Sprintf("%s.tmp", targetFilePath)
	tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		marshaledEntry := proto.MustMarshal(entry)
		size := int64(len(marshaledEntry))
		if err := binary.Write(tempFile, binary.LittleEndian, size); err != nil {
			_ = tempFile.Close()
			return err
		}
		if _, err := tempFile.Write(marshaledEntry); err != nil {
			_ = tempFile.Close()
			return err
		}
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if wal.currentSegment != nil {
		_ = wal.currentSegment.Close()
	}

	_ = os.Remove(targetFilePath)
	if err := os.Rename(tempFilePath, targetFilePath); err != nil {
		return err
	}

	reopened, err := os.OpenFile(targetFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	wal.currentSegment = reopened
	wal.bufWriter = bufio.NewWriter(reopened)

	if len(entries) > 0 {
		wal.CurrSequenceNo = entries[len(entries)-1].Index
		wal.PrevLogTerm = entries[len(entries)-1].Term
	} else {
		wal.CurrSequenceNo = wal.lastSnapshotIndex
	}

	return nil
}

func (w *WAL) ReadRaftLog(idx uint64, maxEntries uint64) ([]*proto.RaftLog, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return nil, err
	}

	var result []*proto.RaftLog
	for _, entry := range entries {
		if entry.Index >= idx {
			result = append(result, entry)
			if maxEntries > 0 && uint64(len(result)) >= maxEntries {
				break
			}
		}
	}
	return result, nil
}

func (w *WAL) RaftLogTerm(idx uint64) (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return 0, err
	}

	for _, entry := range entries {
		if entry.Index == idx {
			return entry.Term, nil
		}
	}

	return 0, nil
}

func (w *WAL) GetLastTermAndIndex() (term uint64, index uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entry, err := w.getLastLogEntryLocked()
	if err != nil {
		return 0, 0, err
	}
	if entry != nil {
		return entry.Term, entry.Index, nil
	}

	snapIdx, snapTerm, err := w.getLatestSnapshotMetadata()
	if err != nil {
		return 0, 0, err
	}
	return snapTerm, snapIdx, nil
}

func (w *WAL) GetFirstTermAndIndex() (term uint64, index uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	snapIdx, snapTerm, err := w.getLatestSnapshotMetadata()
	if err != nil {
		return 0, 0, err
	}

	entry, err := w.getFirstLogEntryLocked()
	if err != nil {
		return 0, 0, err
	}

	if snapIdx > 0 {
		if entry != nil && entry.Index > snapIdx {
			return entry.Term, entry.Index, nil
		}
		allEntries, err := w.readAllFromOffsetLocked(-1)
		if err == nil {
			for _, e := range allEntries {
				if e.Index > snapIdx {
					return e.Term, e.Index, nil
				}
			}
		}
		return snapTerm, snapIdx + 1, nil
	}

	if entry != nil {
		return entry.Term, entry.Index, nil
	}

	return 0, 0, nil
}

func (w *WAL) GetEntriesFrom(startIndex uint64) (entries []*proto.KVPair, prevIndex uint64, prevTerm uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.bufWriter != nil {
		_ = w.bufWriter.Flush()
	}

	allEntries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return nil, 0, 0, err
	}

	if startIndex <= 1 {
		prevIndex = 0
		prevTerm = 0
	} else {
		prevIndex = startIndex - 1
		found := false

		// Check active log entries
		for _, entry := range allEntries {
			if entry.Index == prevIndex {
				prevTerm = entry.Term
				found = true
				break
			}
		}

		if !found {
			// Check if prevIndex corresponds to the latest snapshot
			snapIdx, snapTerm, snapErr := w.getLatestSnapshotMetadata()
			if snapErr == nil && snapIdx > 0 {
				if prevIndex == snapIdx {
					prevTerm = snapTerm
					found = true
				} else if prevIndex < snapIdx {
					return nil, prevIndex, snapTerm, ErrCompacted
				}
			}
		}

		if !found && prevIndex == w.lastSnapshotIndex && w.lastSnapshotIndex > 0 {
			prevTerm = w.PrevLogTerm
			found = true
		}
	}

	for _, entry := range allEntries {
		if entry.Index >= startIndex {
			var valPtr *string
			if entry.Value != nil {
				strVal := string(entry.Value)
				valPtr = &strVal
			}
			entries = append(entries, &proto.KVPair{
				Key:     entry.Key,
				Value:   valPtr,
				Command: entry.Command,
			})
		}
	}
	if entries == nil {
		entries = []*proto.KVPair{}
	}

	return entries, prevIndex, prevTerm, nil
}

func (w *WAL) StreamSnapshot() (lastIncludedTerm uint64, lastIncludedIndex uint64, entries <-chan *proto.KVPair, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	targetPath := w.lastSnapshotPath
	if targetPath == "" {
		files, err := filepath.Glob(filepath.Join(w.snapshotsDir, "snapshot-*"))
		if err != nil {
			return 0, 0, nil, err
		}
		var maxIndex int64 = -1
		for _, f := range files {
			base := filepath.Base(f)
			if strings.HasSuffix(base, ".tmp") || !strings.HasPrefix(base, "snapshot-") {
				continue
			}
			idxStr := strings.TrimPrefix(base, "snapshot-")
			idx, err := strconv.ParseInt(idxStr, 10, 64)
			if err != nil {
				continue
			}
			if idx > maxIndex {
				maxIndex = idx
				targetPath = f
			}
		}
		w.lastSnapshotPath = targetPath
	}

	if targetPath == "" {
		return 0, 0, nil, errors.New("no snapshot found")
	}

	file, err := os.Open(targetPath)
	if err != nil {
		return 0, 0, nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &lastIncludedIndex); err != nil {
		_ = file.Close()
		return 0, 0, nil, fmt.Errorf("reading lastIncludedIndex: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &lastIncludedTerm); err != nil {
		_ = file.Close()
		return 0, 0, nil, fmt.Errorf("reading lastIncludedTerm: %w", err)
	}

	ch := make(chan *proto.KVPair, 64)
	go func() {
		defer file.Close()
		defer close(ch)

		for {
			var size int64
			if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
				break
			}
			data := make([]byte, size)
			if _, err := io.ReadFull(file, data); err != nil {
				break
			}

			var kv proto.KVPair
			if err := googleproto.Unmarshal(data, &kv); err != nil {
				break
			}
			ch <- &kv
		}
	}()

	return lastIncludedTerm, lastIncludedIndex, ch, nil
}
