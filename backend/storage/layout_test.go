package storage

import (
	"os"
	"path/filepath"
	"testing"

	"raft/proto"
)

func TestStorageLayoutKeepsWALAndSnapshotsSeparate(t *testing.T) {
	root := t.TempDir()
	layout := NewStorageLayout(root)
	if err := layout.Ensure(); err != nil {
		t.Fatalf("failed to create layout: %v", err)
	}

	wal, err := OpenWAL(root, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if err := wal.WriteEntry(uint64(i), proto.Command_PUT, "k", []byte("v"), 1); err != nil {
			t.Fatalf("failed to write entry: %v", err)
		}
	}
	if _, err := wal.CreateSnapshot(); err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	if _, err := os.Stat(filepath.Join(layout.WALDir, walFileName)); err != nil {
		t.Fatalf("expected wal.log inside %s: %v", layout.WALDir, err)
	}
	if matches, _ := filepath.Glob(filepath.Join(layout.WALDir, snapshotPrefix+"*")); len(matches) != 0 {
		t.Fatalf("snapshots must not live in the WAL directory: %v", matches)
	}

	matches, err := filepath.Glob(filepath.Join(layout.SnapshotDir, snapshotPrefix+"*"))
	if err != nil {
		t.Fatalf("failed to glob snapshots: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("expected snapshot inside %s", layout.SnapshotDir)
	}
}

func TestProcessStorageDirIsUniquePerProcess(t *testing.T) {
	base := t.TempDir()

	first, err := ProcessStorageDir(base, 1, ":9001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sameIDOtherPort, err := ProcessStorageDir(base, 1, ":9002")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	otherID, err := ProcessStorageDir(base, 2, ":9001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first == sameIDOtherPort || first == otherID || sameIDOtherPort == otherID {
		t.Fatalf("process directories overlap: %q %q %q", first, sameIDOtherPort, otherID)
	}
	if filepath.Dir(first) != base {
		t.Fatalf("expected %q to be under %q", first, base)
	}
}
