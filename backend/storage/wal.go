package storage

import (
	"bufio"
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
	directory         string
	snapshotsDir      string
	logPath           string
	logFile           *os.File
	lock              sync.Mutex
	PrevLogTerm       uint64
	CurrSequenceNo    uint64
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	lastSnapshotPath  string
	bufWriter         *bufio.Writer
	shouldFsync       bool
	maxFileSize       int64
	maxSegments       int64
}

type SnapshotChunk struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	NextOffset        uint64
	TotalSize         uint64
	Data              []byte
}

var (
	ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
	FileNotFound        = errors.New("log file not found")
	ErrCorruptedEntry   = errors.New("CRC mismatch, data might be corrupted")
	ErrCompacted        = errors.New("requested entries have been compacted into snapshot")
)

const (
	walFileName       = "wal.log"
	snapshotPrefix    = "snapshot-"
	snapshotSuffixTmp = ".tmp"
	snapshotHeaderSize = int64(16)
	SnapshotThreshold = uint64(10000)
)

// OpenWAL opens (or creates) the write-ahead log for the process whose
// storage root is root. The WAL lives in <root>/wal and snapshots live in
// <root>/snapshots, so the two never share a directory. When the WAL already
// exists its entries are verified on startup and everything from the first
// corrupt entry onwards is discarded.
func OpenWAL(root string, enableFsync bool, maxFileSize int64, maxSegments int64) (*WAL, error) {
	layout := NewStorageLayout(root)
	if err := layout.Ensure(); err != nil {
		return nil, err
	}

	// Partial snapshot files can only be left behind by a crash; no transfer
	// is active while the WAL is being opened.
	if partials, globErr := filepath.Glob(filepath.Join(layout.SnapshotDir, snapshotPrefix+"*"+snapshotSuffixTmp)); globErr == nil {
		for _, partial := range partials {
			_ = os.Remove(partial)
		}
	}

	logPath := filepath.Join(layout.WALDir, walFileName)
	_, statErr := os.Stat(logPath)
	walExists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		directory:    layout.WALDir,
		snapshotsDir: layout.SnapshotDir,
		logPath:      logPath,
		logFile:      file,
		bufWriter:    bufio.NewWriter(file),
		shouldFsync:  enableFsync,
		maxFileSize:  maxFileSize,
		maxSegments:  maxSegments,
	}

	if err := wal.loadSnapshotMetadataLocked(); err != nil {
		_ = file.Close()
		return nil, err
	}

	if walExists {
		if err := wal.verifyEntriesOnStartupLocked(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	entries, err := wal.readAllFromOffsetLocked(-1)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		if last.Index > wal.lastSnapshotIndex {
			wal.CurrSequenceNo = last.Index
			wal.PrevLogTerm = last.Term
		}
	}

	// A crash can happen after the snapshot file has been committed but
	// before the WAL was truncated. Drop anything the snapshot already
	// covers so that the in-memory log and the WAL stay in sync.
	if wal.lastSnapshotIndex > 0 {
		keep := make([]*proto.RaftLog, 0, len(entries))
		for _, entry := range entries {
			if entry.Index > wal.lastSnapshotIndex {
				keep = append(keep, entry)
			}
		}
		if len(keep) != len(entries) {
			if err := wal.rewriteWALLocked(keep); err != nil {
				_ = file.Close()
				return nil, err
			}
		}
	}

	return wal, nil
}

func (w *WAL) verifyEntriesOnStartupLocked() error {
	file, err := os.Open(w.logFilePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	entries, corrupt := readEntriesUntilCorrupt(file)
	_ = file.Close()

	if !corrupt {
		return nil
	}

	return w.rewriteWALLocked(entries)
}

func readEntriesUntilCorrupt(file *os.File) ([]*proto.RaftLog, bool) {
	var entries []*proto.RaftLog

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			return entries, !errors.Is(err, io.EOF)
		}
		if size <= 0 {
			return entries, true
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return entries, true
		}

		entry, err := unmarshalAndVerify(data)
		if err != nil {
			return entries, true
		}
		entries = append(entries, entry)
	}
}

func renameAtomic(source, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}

	_ = os.Remove(target)
	return os.Rename(source, target)
}

func calculateCRC(val []byte, seq uint64) uint32 {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, seq)
	return crc32.ChecksumIEEE(append(val, buf...))
}

func verifyCRC(entry *proto.RaftLog) bool {
	return entry.CRC == calculateCRC(entry.GetValue(), entry.GetIndex())
}

func (w *WAL) logFilePath() string {
	if w.logPath != "" {
		return w.logPath
	}
	return filepath.Join(w.directory, walFileName)
}

func writeEntryTo(writer io.Writer, entry *proto.RaftLog) error {
	data := proto.MustMarshal(entry)
	if err := binary.Write(writer, binary.LittleEndian, int64(len(data))); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func readEntriesFromFile(file *os.File) ([]*proto.RaftLog, error) {
	var entries []*proto.RaftLog

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return entries, err
		}
		if size <= 0 {
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
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

func readEntriesFromPath(path string) ([]*proto.RaftLog, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	return readEntriesFromFile(file)
}

func (w *WAL) flushLocked() error {
	if w.bufWriter != nil {
		return w.bufWriter.Flush()
	}
	return nil
}

func (w *WAL) readAllFromOffsetLocked(offset int) ([]*proto.RaftLog, error) {
	_ = offset
	if err := w.flushLocked(); err != nil {
		return nil, err
	}
	return readEntriesFromPath(w.logFilePath())
}

func (w *WAL) appendEntryLocked(entry *proto.RaftLog) error {
	if err := writeEntryTo(w.bufWriter, entry); err != nil {
		return err
	}
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if w.shouldFsync {
		return w.logFile.Sync()
	}
	return nil
}

func (w *WAL) WriteEntry(index uint64, cmd proto.Command, key string, val []byte, term uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	entry := &proto.RaftLog{
		Index:   index,
		Term:    term,
		Key:     key,
		Value:   val,
		Command: cmd,
		CRC:     calculateCRC(val, index),
	}

	if err := w.appendEntryLocked(entry); err != nil {
		return err
	}

	w.CurrSequenceNo = index
	w.PrevLogTerm = term

	if w.CurrSequenceNo >= w.lastSnapshotIndex+SnapshotThreshold {
		if _, err := w.createSnapshotFileLocked(); err != nil {
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

func (w *WAL) createSnapshotFileLocked() (string, error) {
	if err := w.flushLocked(); err != nil {
		return "", err
	}

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		if w.lastSnapshotPath != "" {
			return w.lastSnapshotPath, nil
		}
		return "", errors.New("no entries to snapshot")
	}

	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.Index
	lastTerm := lastEntry.Term

	if lastIndex <= w.lastSnapshotIndex && w.lastSnapshotPath != "" {
		return w.lastSnapshotPath, nil
	}

	kvMap := make(map[string][]byte)
	if w.lastSnapshotIndex > 0 {
		if err := w.loadSnapshotKVsLocked(kvMap); err != nil {
			return "", fmt.Errorf("loading previous snapshot: %w", err)
		}
	}
	for _, entry := range entries {
		if entry.Index <= w.lastSnapshotIndex {
			continue
		}
		switch entry.GetCommand() {
		case proto.Command_PUT:
			kvMap[entry.GetKey()] = entry.GetValue()
		case proto.Command_DELETE:
			delete(kvMap, entry.GetKey())
		}
	}

	if err := os.MkdirAll(w.snapshotsDir, 0755); err != nil {
		return "", err
	}

	targetFileName := fmt.Sprintf("%s%d", snapshotPrefix, lastIndex)
	targetFilePath := filepath.Join(w.snapshotsDir, targetFileName)
	tempFilePath := targetFilePath + snapshotSuffixTmp

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

	for _, key := range keys {
		strVal := string(kvMap[key])
		snapEntry := &proto.KVPair{
			Key:     key,
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

	if err := renameAtomic(tempFilePath, targetFilePath); err != nil {
		_ = os.Remove(tempFilePath)
		return "", fmt.Errorf("committing snapshot file: %w", err)
	}

	w.lastSnapshotIndex = lastIndex
	w.lastSnapshotTerm = lastTerm
	w.lastSnapshotPath = targetFilePath

	remaining := make([]*proto.RaftLog, 0, len(entries))
	for _, entry := range entries {
		if entry.Index > lastIndex {
			remaining = append(remaining, entry)
		}
	}
	if err := w.rewriteWALLocked(remaining); err != nil {
		return targetFilePath, err
	}

	w.deleteOldSnapshotsLocked(targetFilePath)

	return targetFilePath, nil
}

func (w *WAL) loadSnapshotMetadataLocked() error {
	index, term, err := w.getLatestSnapshotMetadata()
	if err != nil {
		return err
	}
	if index > 0 {
		w.lastSnapshotIndex = index
		w.lastSnapshotTerm = term
		w.PrevLogTerm = term
		w.CurrSequenceNo = index
	}
	return nil
}

func (w *WAL) LatestSnapshotMetadata() (lastIncludedIndex uint64, lastIncludedTerm uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.getLatestSnapshotMetadata()
}

func (w *WAL) getLatestSnapshotMetadata() (uint64, uint64, error) {
	files, err := filepath.Glob(filepath.Join(w.snapshotsDir, snapshotPrefix+"*"))
	if err != nil {
		return 0, 0, err
	}

	var maxIndex int64 = -1
	var latestFile string
	for _, f := range files {
		base := filepath.Base(f)
		if strings.HasSuffix(base, snapshotSuffixTmp) || !strings.HasPrefix(base, snapshotPrefix) {
			continue
		}
		index, err := strconv.ParseInt(strings.TrimPrefix(base, snapshotPrefix), 10, 64)
		if err != nil {
			continue
		}
		if index > maxIndex {
			maxIndex = index
			latestFile = f
		}
	}

	if latestFile == "" {
		return 0, 0, nil
	}
	w.lastSnapshotPath = latestFile

	file, err := os.Open(latestFile)
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

// SnapshotDirectory is the directory that holds the snapshot files.
func (w *WAL) SnapshotDirectory() string {
	return w.snapshotsDir
}

func (w *WAL) ReadSnapshotChunk(offset uint64, maxBytes int) (*SnapshotChunk, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.lastSnapshotPath == "" {
		index, _, err := w.getLatestSnapshotMetadata()
		if err != nil {
			return nil, err
		}
		if index == 0 {
			return nil, errors.New("no snapshot found")
		}
	}

	file, err := os.Open(w.lastSnapshotPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lastIndex, lastTerm uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &lastTerm); err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	bodySize := info.Size() - snapshotHeaderSize
	if bodySize < 0 {
		return nil, errors.New("snapshot file is missing its header")
	}

	chunk := &SnapshotChunk{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Offset:            offset,
		NextOffset:        offset,
		TotalSize:         uint64(bodySize),
	}
	if offset >= uint64(bodySize) {
		return chunk, nil
	}

	if maxBytes <= 0 {
		maxBytes = 64 * 1024
	}
	remaining := bodySize - int64(offset)
	readSize := int64(maxBytes)
	readSize = min(readSize, remaining)

	if _, err := file.Seek(snapshotHeaderSize+int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, readSize)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}

	chunk.Data = data
	chunk.NextOffset = offset + uint64(readSize)
	return chunk, nil
}

func (w *WAL) InstallSnapshot(tempPath string, lastIncludedIndex, lastIncludedTerm uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := os.MkdirAll(w.snapshotsDir, 0755); err != nil {
		return err
	}

	targetPath := filepath.Join(w.snapshotsDir, fmt.Sprintf("%s%d", snapshotPrefix, lastIncludedIndex))
	if err := renameAtomic(tempPath, targetPath); err != nil {
		return err
	}

	w.lastSnapshotIndex = lastIncludedIndex
	w.lastSnapshotTerm = lastIncludedTerm
	w.lastSnapshotPath = targetPath

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return err
	}
	keep := make([]*proto.RaftLog, 0, len(entries))
	for _, entry := range entries {
		if entry.Index > lastIncludedIndex {
			keep = append(keep, entry)
		}
	}
	if err := w.rewriteWALLocked(keep); err != nil {
		return err
	}

	w.deleteOldSnapshotsLocked(targetPath)
	return nil
}

func (w *WAL) loadSnapshotKVsLocked(kvMap map[string][]byte) error {
	if w.lastSnapshotIndex == 0 {
		return nil
	}
	if w.lastSnapshotPath == "" {
		index, _, err := w.getLatestSnapshotMetadata()
		if err != nil {
			return err
		}
		if index == 0 {
			return errors.New("previous snapshot file not found")
		}
	}

	file, err := os.Open(w.lastSnapshotPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var lastIndex, lastTerm uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		return err
	}
	if err := binary.Read(file, binary.LittleEndian, &lastTerm); err != nil {
		return err
	}

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if size <= 0 {
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return err
		}

		var kv proto.KVPair
		if err := googleproto.Unmarshal(data, &kv); err != nil {
			return err
		}

		switch kv.GetCommand() {
		case proto.Command_DELETE:
			delete(kvMap, kv.GetKey())
		default:
			if kv.Value == nil {
				kvMap[kv.GetKey()] = nil
			} else {
				kvMap[kv.GetKey()] = []byte(kv.GetValue())
			}
		}
	}

	return nil
}

func (w *WAL) deleteOldSnapshotsLocked(keepPath string) {
	files, err := filepath.Glob(filepath.Join(w.snapshotsDir, snapshotPrefix+"*"))
	if err != nil {
		return
	}

	keepPath = filepath.Clean(keepPath)
	for _, file := range files {
		if filepath.Clean(file) == keepPath {
			continue
		}

		if strings.HasSuffix(file, snapshotSuffixTmp) {
			continue
		}
		_ = os.Remove(file)
	}
}

func (w *WAL) rewriteWALLocked(entries []*proto.RaftLog) error {
	if err := w.flushLocked(); err != nil {
		return err
	}

	tempPath := w.logFilePath() + snapshotSuffixTmp
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	cleanup := func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}

	for _, entry := range entries {
		if err := writeEntryTo(tempFile, entry); err != nil {
			cleanup()
			return err
		}
	}

	if err := tempFile.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	if w.logFile != nil {
		_ = w.logFile.Close()
	}

	if err := renameAtomic(tempPath, w.logFilePath()); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	reopened, err := os.OpenFile(w.logFilePath(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.logFile = reopened
	w.bufWriter = bufio.NewWriter(reopened)

	if len(entries) > 0 {
		last := entries[len(entries)-1]
		w.CurrSequenceNo = last.Index
		w.PrevLogTerm = last.Term
	} else {
		w.CurrSequenceNo = w.lastSnapshotIndex
		w.PrevLogTerm = w.lastSnapshotTerm
	}

	return nil
}

func (w *WAL) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.flushLocked(); err != nil {
		if w.logFile != nil {
			_ = w.logFile.Close()
		}
		return err
	}
	if w.logFile == nil {
		return nil
	}
	if w.shouldFsync {
		_ = w.logFile.Sync()
	}
	return w.logFile.Close()
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

func (w *WAL) Repair(idx uint64) ([]*proto.RaftLog, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return nil, err
	}

	keep := make([]*proto.RaftLog, 0, len(entries))
	for _, entry := range entries {
		if entry.Index >= idx {
			continue
		}
		keep = append(keep, entry)
	}

	if len(keep) == len(entries) {
		return entries, nil
	}

	if err := w.rewriteWALLocked(keep); err != nil {
		return keep, err
	}

	return keep, nil
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

	if idx == w.lastSnapshotIndex && w.lastSnapshotIndex > 0 {
		return w.lastSnapshotTerm, nil
	}

	return 0, nil
}

func (w *WAL) GetLastTermAndIndex() (term uint64, index uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return 0, 0, err
	}
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		return last.Term, last.Index, nil
	}

	return w.lastSnapshotTerm, w.lastSnapshotIndex, nil
}

func (w *WAL) GetFirstTermAndIndex() (term uint64, index uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	entries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return 0, 0, err
	}

	if len(entries) > 0 {
		first := entries[0]
		return first.Term, first.Index, nil
	}

	if w.lastSnapshotIndex > 0 {
		// The first index that is not part of the snapshot.
		return w.lastSnapshotTerm, w.lastSnapshotIndex + 1, nil
	}

	return 0, 0, nil
}

func (w *WAL) GetEntriesFrom(startIndex uint64) (
	entries []*proto.KVPair,
	prevIndex uint64,
	prevTerm uint64,
	err error,
) {
	w.lock.Lock()
	defer w.lock.Unlock()

	allEntries, err := w.readAllFromOffsetLocked(-1)
	if err != nil {
		return nil, 0, 0, err
	}

	if startIndex <= 1 {
		prevIndex = 0
		prevTerm = 0
	} else {
		prevIndex = startIndex - 1

		if w.lastSnapshotIndex > 0 {
			if prevIndex < w.lastSnapshotIndex {
				return nil, prevIndex, w.lastSnapshotTerm, ErrCompacted
			}
			if prevIndex == w.lastSnapshotIndex {
				prevTerm = w.lastSnapshotTerm
			}
		}

		if prevIndex != w.lastSnapshotIndex || w.lastSnapshotIndex == 0 {
			for _, entry := range allEntries {
				if entry.Index == prevIndex {
					prevTerm = entry.Term
					break
				}
			}
		}
	}

	entries = make([]*proto.KVPair, 0)
	for _, entry := range allEntries {
		if entry.Index < startIndex {
			continue
		}

		var valPtr *string
		if entry.Value != nil {
			strVal := string(entry.Value)
			valPtr = &strVal
		}
		entries = append(entries, &proto.KVPair{
			Term:    entry.Term,
			Index:   entry.Index,
			Key:     entry.Key,
			Value:   valPtr,
			Command: entry.Command,
		})
	}

	return entries, prevIndex, prevTerm, nil
}

func (w *WAL) StreamSnapshot() (
	lastIncludedTerm uint64,
	lastIncludedIndex uint64,
	entries <-chan *proto.KVPair,
	err error,
) {
	w.lock.Lock()

	targetPath := w.lastSnapshotPath
	if targetPath == "" {
		index, _, metaErr := w.getLatestSnapshotMetadata()
		if metaErr != nil {
			w.lock.Unlock()
			return 0, 0, nil, metaErr
		}
		if index == 0 {
			w.lock.Unlock()
			return 0, 0, nil, errors.New("no snapshot found")
		}
		targetPath = w.lastSnapshotPath
	}

	file, err := os.Open(targetPath)
	if err != nil {
		w.lock.Unlock()
		return 0, 0, nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &lastIncludedIndex); err != nil {
		_ = file.Close()
		w.lock.Unlock()
		return 0, 0, nil, fmt.Errorf("reading lastIncludedIndex: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &lastIncludedTerm); err != nil {
		_ = file.Close()
		w.lock.Unlock()
		return 0, 0, nil, fmt.Errorf("reading lastIncludedTerm: %w", err)
	}

	var snapshotEntries []*proto.KVPair
	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			break
		}
		if size <= 0 {
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
		snapshotEntries = append(snapshotEntries, &kv)
	}
	_ = file.Close()
	w.lock.Unlock()

	ch := make(chan *proto.KVPair)
	go func() {
		defer close(ch)
		for _, kv := range snapshotEntries {
			ch <- kv
		}
	}()

	return lastIncludedTerm, lastIncludedIndex, ch, nil
}
