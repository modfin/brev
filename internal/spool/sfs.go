package spool

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type SFS interface {
	Rename(oldpath, newpath string) error
	Remove(path string) error

	OpenReader(path string) (io.ReadCloser, error)
	OpenWrite(path string, append bool) (io.WriteCloser, error)

	Walk(root string, fn func(path string))
	Exist(filePath string) bool
}

type SFSOpenReader interface {
	OpenReader(path string) (io.ReadCloser, error)
}

func ReadAll(path string, oppener SFSOpenReader) ([]byte, error) {
	r, err := oppener.OpenReader(path)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

type LocalFS struct {
	root string
}

func (l LocalFS) Remove(path string) error {
	return os.Remove(path)
}

func (l LocalFS) Walk(root string, fn func(path string)) {

	dir := filepath.Join(l.root, root)
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		p, err := filepath.Rel(l.root, path)
		if err != nil {
			fmt.Println("could not get relative path", err, "for", path, "relative to", l.root)
		}
		fn(p)
		return nil
	})
}

func (l LocalFS) Rename(oldpath, newpath string) error {
	oldpath = filepath.Join(l.root, oldpath)
	newpath = filepath.Join(l.root, newpath)
	err := os.MkdirAll(filepath.Dir(newpath), 0755)
	if err != nil {
		return err
	}
	return os.Rename(oldpath, newpath)
}

func (l LocalFS) OpenReader(file string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.root, file))
}

func (l LocalFS) OpenWrite(path string, append bool) (io.WriteCloser, error) {

	full := filepath.Join(l.root, path)

	err := os.MkdirAll(filepath.Dir(full), 0755)
	if err != nil {
		return nil, err
	}
	if append {
		return os.OpenFile(full, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
	}
	return os.OpenFile(full, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
}

func (l LocalFS) Exist(filePath string) bool {
	filePath = filepath.Join(l.root, filePath)
	_, err := os.Stat(filePath)
	return !errors.Is(err, os.ErrNotExist)
}

func NewLocalFS(root string) (SFS, error) {

	l := &LocalFS{root: root}
	for _, dir := range catagories {
		var err error
		err = os.MkdirAll(filepath.Join(root, dir), 0755)
		if err != nil {
			return nil, fmt.Errorf("could not create directory: %w", err)
		}
	}

	return l, nil
}
