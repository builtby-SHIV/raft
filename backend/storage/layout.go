package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	dataDirName     = "data"
	walDirName      = "wal"
	snapshotDirName = "snapshots"
	sstableDirName  = "sstable"
	configDirName   = "config"
)

type StorageLayout struct {
	Root        string
	WALDir      string
	SnapshotDir string
	SSTableDir  string
	ConfigDir   string
}

func NewStorageLayout(root string) StorageLayout {
	return StorageLayout{
		Root:        root,
		WALDir:      filepath.Join(root, walDirName),
		SnapshotDir: filepath.Join(root, snapshotDirName),
		SSTableDir:  filepath.Join(root, sstableDirName),
		ConfigDir:   filepath.Join(root, configDirName),
	}
}

func (l StorageLayout) Ensure() error {
	for _, dir := range []string{l.Root, l.WALDir, l.SnapshotDir, l.SSTableDir, l.ConfigDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating storage directory %s: %w", dir, err)
		}
	}
	return nil
}

func DefaultStorageBaseDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return dataDirName
	}
	return filepath.Join(cwd, dataDirName)
}

func ProcessStorageDir(baseDir string, nodeID uint64, tcpAddr string) (string, error) {
	port, err := ParsePort(tcpAddr)
	if err != nil {
		return "", err
	}
	if port == 0 {
		return filepath.Join(baseDir, fmt.Sprintf("node-%d", nodeID)), nil
	}
	return filepath.Join(baseDir, fmt.Sprintf("node-%d-%d", nodeID, port)), nil
}
