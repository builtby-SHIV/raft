package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"raft/proto"
)

func TestOpenWALAndWriteEntries(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, true, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	if err := wal.WriteEntry(proto.Command_PUT, "user:1", []byte("alice"), 1); err != nil {
		t.Fatalf("failed to write put entry: %v", err)
	}
	if err := wal.WriteEntry(proto.Command_PUT, "user:2", []byte("bob"), 1); err != nil {
		t.Fatalf("failed to write put entry: %v", err)
	}
	if err := wal.WriteEntry(proto.Command_DELETE, "user:1", nil, 2); err != nil {
		t.Fatalf("failed to write delete entry: %v", err)
	}

	entries, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("failed to read all entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Index != 1 || entries[0].Term != 1 || entries[0].Key != "user:1" || string(entries[0].Value) != "alice" || entries[0].Command != proto.Command_PUT {
		t.Fatalf("unexpected entry 0: %+v", entries[0])
	}
	if entries[1].Index != 2 || entries[1].Term != 1 || entries[1].Key != "user:2" || string(entries[1].Value) != "bob" || entries[1].Command != proto.Command_PUT {
		t.Fatalf("unexpected entry 1: %+v", entries[1])
	}
	if entries[2].Index != 3 || entries[2].Term != 2 || entries[2].Key != "user:1" || entries[2].Command != proto.Command_DELETE {
		t.Fatalf("unexpected entry 2: %+v", entries[2])
	}

	term, err := wal.RaftLogTerm(2)
	if err != nil || term != 1 {
		t.Fatalf("expected term 1 for index 2, got %d, err: %v", term, err)
	}
}

func TestSnapshotCreationAndMetadata(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-snap-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	for i := 1; i <= 5; i++ {
		cmd := proto.Command_PUT
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		term := uint64(1)
		if i == 5 {
			term = 2
		}
		if err := wal.WriteEntry(cmd, key, []byte(val), term); err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}

	// Trigger snapshot manually
	snapPath, err := wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	_ = wal.Close()

	snapshotsDir := filepath.Join(tempDir, "snapshots")
	expectedFileName := "snapshot-5"
	expectedFilePath := filepath.Join(snapshotsDir, expectedFileName)

	if snapPath != expectedFilePath {
		t.Fatalf("expected snapshot path %s, got %s", expectedFilePath, snapPath)
	}

	file, err := os.Open(expectedFilePath)
	if err != nil {
		t.Fatalf("failed to open snapshot file: %v", err)
	}
	defer file.Close()

	var lastIndex, lastTerm uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		t.Fatalf("failed to read lastIndex from snapshot: %v", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &lastTerm); err != nil {
		t.Fatalf("failed to read lastTerm from snapshot: %v", err)
	}

	if lastIndex != 5 {
		t.Errorf("expected lastIndex=5, got %d", lastIndex)
	}
	if lastTerm != 2 {
		t.Errorf("expected lastTerm=2, got %d", lastTerm)
	}

	// Verify no .tmp files exist in snapshots directory (atomicity check)
	tmpFiles, err := filepath.Glob(filepath.Join(snapshotsDir, "*.tmp"))
	if err != nil {
		t.Fatalf("failed to check for tmp files: %v", err)
	}
	if len(tmpFiles) > 0 {
		t.Errorf("expected no tmp files, found: %v", tmpFiles)
	}
}

func TestSnapshotCrossing10000Entries(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-10k-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 10*1024*1024, 100)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	for i := 1; i <= 10000; i++ {
		cmd := proto.Command_PUT
		key := fmt.Sprintf("k-%d", i%500)
		val := fmt.Sprintf("v-%d", i)
		term := uint64(3)
		if err := wal.WriteEntry(cmd, key, []byte(val), term); err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}

	snapshotsDir := filepath.Join(tempDir, "snapshots")
	expectedFileName := "snapshot-10000"
	expectedFilePath := filepath.Join(snapshotsDir, expectedFileName)

	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Fatalf("expected snapshot file %s to exist", expectedFilePath)
	}

	file, err := os.Open(expectedFilePath)
	if err != nil {
		t.Fatalf("failed to open snapshot file: %v", err)
	}
	defer file.Close()

	var lastIndex, lastTerm uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		t.Fatalf("failed to read lastIndex: %v", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &lastTerm); err != nil {
		t.Fatalf("failed to read lastTerm: %v", err)
	}

	if lastIndex != 10000 {
		t.Errorf("expected lastIndex=10000, got %d", lastIndex)
	}
	if lastTerm != 3 {
		t.Errorf("expected lastTerm=3, got %d", lastTerm)
	}
}

func TestRepair(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-repair-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, true, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	for i := 1; i <= 5; i++ {
		_ = wal.WriteEntry(proto.Command_PUT, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)), 1)
	}

	// Repair from index 4: entries 4 and 5 should be truncated
	entries, err := wal.Repair(4)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 remaining entries after repair, got %d", len(entries))
	}
	if wal.CurrSequenceNo != 3 {
		t.Errorf("expected CurrSequenceNo=3, got %d", wal.CurrSequenceNo)
	}
}

func TestGetLastTermAndIndex(t *testing.T) {
	// 1. Empty log with no snapshot
	tempDir, err := os.MkdirTemp("", "wal-last-term-idx-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	term, idx, err := wal.GetLastTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error on empty log: %v", err)
	}
	if term != 0 || idx != 0 {
		t.Fatalf("expected term=0, idx=0; got term=%d, idx=%d", term, idx)
	}

	// 2. Log has entries
	if err := wal.WriteEntry(proto.Command_PUT, "k1", []byte("v1"), 1); err != nil {
		t.Fatalf("failed to write entry 1: %v", err)
	}
	if err := wal.WriteEntry(proto.Command_PUT, "k2", []byte("v2"), 3); err != nil {
		t.Fatalf("failed to write entry 2: %v", err)
	}

	term, idx, err = wal.GetLastTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error on log with entries: %v", err)
	}
	if term != 3 || idx != 2 {
		t.Fatalf("expected term=3, idx=2; got term=%d, idx=%d", term, idx)
	}

	// 3. Snapshot created, then log emptied (truncated/repaired from index 1)
	_, err = wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Repair from index 1 removes all log entries from the segment
	_, err = wal.Repair(1)
	if err != nil {
		t.Fatalf("failed to repair: %v", err)
	}

	// Log is now empty, so it should read last term and index from snapshot (term 3, index 2)
	term, idx, err = wal.GetLastTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error reading from snapshot: %v", err)
	}
	if term != 3 || idx != 2 {
		t.Fatalf("expected term=3, idx=2 from snapshot; got term=%d, idx=%d", term, idx)
	}

	// 4. Log appends a new entry after the snapshot
	if err := wal.WriteEntry(proto.Command_PUT, "k3", []byte("v3"), 5); err != nil {
		t.Fatalf("failed to write entry after snapshot: %v", err)
	}
	term, idx, err = wal.GetLastTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 5 || idx != 3 {
		t.Fatalf("expected term=5, idx=3; got term=%d, idx=%d", term, idx)
	}
}

func TestGetFirstTermAndIndex(t *testing.T) {
	// 1. Empty log with no snapshot
	tempDir, err := os.MkdirTemp("", "wal-first-term-idx-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	term, idx, err := wal.GetFirstTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error on empty log: %v", err)
	}
	if term != 0 || idx != 0 {
		t.Fatalf("expected term=0, idx=0; got term=%d, idx=%d", term, idx)
	}

	// 2. Log has entries
	if err := wal.WriteEntry(proto.Command_PUT, "k1", []byte("v1"), 1); err != nil {
		t.Fatalf("failed to write entry 1: %v", err)
	}
	if err := wal.WriteEntry(proto.Command_PUT, "k2", []byte("v2"), 3); err != nil {
		t.Fatalf("failed to write entry 2: %v", err)
	}

	term, idx, err = wal.GetFirstTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error on log with entries: %v", err)
	}
	if term != 1 || idx != 1 {
		t.Fatalf("expected term=1, idx=1; got term=%d, idx=%d", term, idx)
	}

	// 3. Snapshot created, then log emptied (truncated/repaired from index 1)
	_, err = wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	_, err = wal.Repair(1)
	if err != nil {
		t.Fatalf("failed to repair: %v", err)
	}

	// Log is now empty, snapshot has lastIncludedIndex=2, lastIncludedTerm=3
	// First uncompacted index is 3
	term, idx, err = wal.GetFirstTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error reading first term/index after snapshot: %v", err)
	}
	if term != 3 || idx != 3 {
		t.Fatalf("expected term=3, idx=3 after snapshot; got term=%d, idx=%d", term, idx)
	}

	// 4. Log appends a new entry after snapshot
	if err := wal.WriteEntry(proto.Command_PUT, "k3", []byte("v3"), 5); err != nil {
		t.Fatalf("failed to write entry after snapshot: %v", err)
	}
	term, idx, err = wal.GetFirstTermAndIndex()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 5 || idx != 3 {
		t.Fatalf("expected term=5, idx=3; got term=%d, idx=%d", term, idx)
	}
}

func TestGetEntriesFrom(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-get-entries-from-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	// Write entries:
	// Index 1, Term 1
	// Index 2, Term 1
	// Index 3, Term 2
	// Index 4, Term 2
	// Index 5, Term 3
	_ = wal.WriteEntry(proto.Command_PUT, "k1", []byte("v1"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "k2", []byte("v2"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "k3", []byte("v3"), 2)
	_ = wal.WriteEntry(proto.Command_PUT, "k4", []byte("v4"), 2)
	_ = wal.WriteEntry(proto.Command_PUT, "k5", []byte("v5"), 3)

	// Case 1: startIndex = 1 (start of log)
	entries, prevIdx, prevTerm, err := wal.GetEntriesFrom(1)
	if err != nil {
		t.Fatalf("unexpected error for startIndex 1: %v", err)
	}
	if prevIdx != 0 || prevTerm != 0 {
		t.Fatalf("expected prevIdx=0, prevTerm=0; got prevIdx=%d, prevTerm=%d", prevIdx, prevTerm)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	if entries[0].Key != "k1" || entries[0].GetValue() != "v1" || entries[4].Key != "k5" || entries[4].GetValue() != "v5" {
		t.Fatalf("unexpected entries: %+v ... %+v", entries[0], entries[4])
	}

	// Case 2: startIndex = 3 (middle of log)
	entries, prevIdx, prevTerm, err = wal.GetEntriesFrom(3)
	if err != nil {
		t.Fatalf("unexpected error for startIndex 3: %v", err)
	}
	if prevIdx != 2 || prevTerm != 1 {
		t.Fatalf("expected prevIdx=2, prevTerm=1; got prevIdx=%d, prevTerm=%d", prevIdx, prevTerm)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (k3,k4,k5), got %d", len(entries))
	}
	if entries[0].Key != "k3" || entries[1].Key != "k4" || entries[2].Key != "k5" {
		t.Fatalf("unexpected entry keys: %+v", entries)
	}

	// Case 3: startIndex = 6 (heartbeat / end of log)
	entries, prevIdx, prevTerm, err = wal.GetEntriesFrom(6)
	if err != nil {
		t.Fatalf("unexpected error for startIndex 6: %v", err)
	}
	if prevIdx != 5 || prevTerm != 3 {
		t.Fatalf("expected prevIdx=5, prevTerm=3; got prevIdx=%d, prevTerm=%d", prevIdx, prevTerm)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}

	// Case 4: Snapshot at index 5, then repair/truncate and add entry 6
	_, err = wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Truncate log segments up to index 5
	_, err = wal.Repair(1)
	if err != nil {
		t.Fatalf("failed to repair: %v", err)
	}

	// Now append entry 6, term 4
	_ = wal.WriteEntry(proto.Command_PUT, "k6", []byte("v6"), 4)

	// startIndex = 6: entry 6 is in log, entry 5 is from the snapshot (lastIndex 5, lastTerm 3)
	entries, prevIdx, prevTerm, err = wal.GetEntriesFrom(6)
	if err != nil {
		t.Fatalf("unexpected error for startIndex 6 with snapshot: %v", err)
	}
	if prevIdx != 5 || prevTerm != 3 {
		t.Fatalf("expected prevIdx=5, prevTerm=3 from snapshot; got prevIdx=%d, prevTerm=%d", prevIdx, prevTerm)
	}
	if len(entries) != 1 || entries[0].Key != "k6" || entries[0].GetValue() != "v6" {
		t.Fatalf("expected 1 entry with key k6, val v6; got %+v", entries)
	}

	// Case 5: startIndex < snapshot lastIndex (compacted)
	_, _, _, err = wal.GetEntriesFrom(2)
	if err == nil || !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted for index 2, got %v", err)
	}
}

func TestStreamSnapshot(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-stream-snap-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	// 1. When no snapshot exists, should return error
	_, _, _, err = wal.StreamSnapshot()
	if err == nil {
		t.Fatalf("expected error streaming when no snapshot exists, got nil")
	}

	// 2. Write entries:
	// PUT a = 1
	// PUT b = 2
	// PUT c = 3
	// PUT a = 10 (overwrite)
	// DELETE b
	_ = wal.WriteEntry(proto.Command_PUT, "a", []byte("1"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "b", []byte("2"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "c", []byte("3"), 2)
	_ = wal.WriteEntry(proto.Command_PUT, "a", []byte("10"), 2)
	_ = wal.WriteEntry(proto.Command_DELETE, "b", nil, 3)

	_, err = wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Stream from latest snapshot (no path argument)
	lastTerm, lastIdx, ch, err := wal.StreamSnapshot()
	if err != nil {
		t.Fatalf("unexpected error streaming snapshot: %v", err)
	}

	if lastTerm != 3 || lastIdx != 5 {
		t.Fatalf("expected lastIncludedTerm=3, lastIncludedIndex=5; got term=%d, index=%d", lastTerm, lastIdx)
	}

	var streamed []*proto.KVPair
	for kv := range ch {
		streamed = append(streamed, kv)
	}

	// Verify that channel is closed
	if _, open := <-ch; open {
		t.Fatalf("expected stream channel to be closed after reading all entries")
	}

	// Active keys in snapshot should be "a"="10" and "c"="3" (b was deleted)
	if len(streamed) != 2 {
		t.Fatalf("expected 2 streamed KVPairs, got %d: %+v", len(streamed), streamed)
	}
	if streamed[0].Key != "a" || streamed[0].GetValue() != "10" || streamed[0].Command != proto.Command_PUT {
		t.Fatalf("unexpected first entry: %+v", streamed[0])
	}
	if streamed[1].Key != "c" || streamed[1].GetValue() != "3" || streamed[1].Command != proto.Command_PUT {
		t.Fatalf("unexpected second entry: %+v", streamed[1])
	}

	// Verify streaming from a reopened WAL on disk (no path argument)
	wal2, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer wal2.Close()

	lastTerm2, lastIdx2, ch2, err := wal2.StreamSnapshot()
	if err != nil {
		t.Fatalf("unexpected error streaming snapshot from reopened wal: %v", err)
	}
	if lastTerm2 != 3 || lastIdx2 != 5 {
		t.Fatalf("expected term 3 index 5, got %d, %d", lastTerm2, lastIdx2)
	}

	count := 0
	for range ch2 {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 items, got %d", count)
	}
}
