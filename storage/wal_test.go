package storage

import (
	"encoding/binary"
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
	defer wal.Close()

	// Use smaller threshold to test the snapshot mechanism quickly
	wal.SetSnapshotThreshold(5)

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

	snapshotsDir := filepath.Join(tempDir, "snapshots")
	expectedFileName := "snapshot-5"
	expectedFilePath := filepath.Join(snapshotsDir, expectedFileName)

	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Fatalf("expected snapshot file %s to exist in %s", expectedFileName, snapshotsDir)
	}

	// Verify metadata at the start: lastIndex (uint64) and lastTerm (uint64)
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

	// Read snapshot via helper
	sIdx, sTerm, entries, err := ReadSnapshot(expectedFilePath)
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}
	if sIdx != 5 || sTerm != 2 {
		t.Errorf("snapshot metadata mismatch: index=%d, term=%d", sIdx, sTerm)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 snapshotted entries, got %d", len(entries))
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

	// DefaultSnapshotThreshold is 10000
	if wal.snapshotThreshold != 10000 {
		t.Fatalf("expected DefaultSnapshotThreshold to be 10000, got %d", wal.snapshotThreshold)
	}

	for i := 1; i <= 10000; i++ {
		cmd := proto.Command_PUT
		key := fmt.Sprintf("k-%d", i%500) // overwrite keys to test state compaction
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

	lastIndex, lastTerm, err := ReadSnapshotMetadata(expectedFilePath)
	if err != nil {
		t.Fatalf("failed to read snapshot metadata: %v", err)
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

func TestSnapshotWithPutAndDeleteState(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-snap-state-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	wal.SetSnapshotThreshold(4)

	_ = wal.WriteEntry(proto.Command_PUT, "foo", []byte("bar1"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "foo", []byte("bar2"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "baz", []byte("qux"), 1)
	// 4th entry triggers snapshot
	_ = wal.WriteEntry(proto.Command_DELETE, "foo", nil, 2)

	_ = wal.Close()

	// Check snapshot file
	snapPath := filepath.Join(tempDir, "snapshots", "snapshot-4")
	lastIdx, lastTerm, entries, err := ReadSnapshot(snapPath)
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}
	if lastIdx != 4 || lastTerm != 2 {
		t.Fatalf("expected lastIndex=4, lastTerm=2; got index=%d, term=%d", lastIdx, lastTerm)
	}

	// "foo" was deleted, so only "baz" should remain in compacted snapshot state
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in snapshot state, got %d", len(entries))
	}
	if entries[0].Key != "baz" || string(entries[0].Value) != "qux" {
		t.Fatalf("unexpected entry in snapshot: %+v", entries[0])
	}

	// Reopen WAL and verify it recovers the state and sequences
	reopened, err := OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer reopened.Close()

	if reopened.CurrSequenceNo < 4 {
		t.Fatalf("expected CurrSequenceNo >= 4 after recovery, got %d", reopened.CurrSequenceNo)
	}
}
