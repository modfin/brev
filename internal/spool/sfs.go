package spool

import (
	"errors"
	"fmt"
	"github.com/modfin/brev/pkg/zid"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/slicez"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SFS interface {
	Move(tid string, fromCategory string, toCategory string) error
	Remove(category string, path string) error

	OpenReader(category string, tid string) (io.ReadCloser, error)
	OpenWriter(category string, tid string) (io.WriteCloser, error)

	Walk(category string, fn func(tid string))
	Exist(category string, tid string) bool

	Lock(tid string)
	Unlock(tid string)
}

type SFSOpenReader interface {
	OpenReader(category string, tid string) (io.ReadCloser, error)
}

func ReadAll(category string, tid string, oppener SFSOpenReader) ([]byte, error) {
	r, err := oppener.OpenReader(category, tid)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

func NewLocalFS(root string) (SFS, error) {

	l := &LocalFS{root: root, mu: tools.NewKeyedMutex()}
	for _, dir := range catagories {
		var err error
		err = os.MkdirAll(filepath.Join(root, dir), 0755)
		if err != nil {
			return nil, fmt.Errorf("could not create directory: %w", err)
		}
	}

	go l.start()

	return l, nil
}

type LocalFS struct {
	root string
	mu   *tools.KeyedMutex
}

func (l LocalFS) start() {

	isDirEmpty := func(path string) bool {
		f, err := os.Open(path)
		if err != nil {
			return false
		}
		defer f.Close()

		_, err = f.Readdirnames(1)
		return err == io.EOF
	}

	dirsIn := func(path string) []string {
		e, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		return slicez.Map(slicez.Filter(e, func(d fs.DirEntry) bool {
			return d.IsDir()
		}), func(d fs.DirEntry) string {
			return d.Name()
		})
	}

	var walk func(string)
	walk = func(path string) {
		for _, name := range dirsIn(path) {
			walk(filepath.Join(path, name))
		}

		if isDirEmpty(path) {
			fmt.Println("removing empty dir", path)
			err := os.Remove(path)
			if err != nil {
				fmt.Println("could not remove empty dir", path, err)
			}
		}
	}

	for {
		for _, dir := range catagories {
			for _, name := range dirsIn(filepath.Join(l.root, dir)) {
				walk(filepath.Join(l.root, dir, name))
			}
		}
		time.Sleep(24 * time.Hour)
	}
}

func (l LocalFS) Lock(tid string) {
	l.mu.Lock(tid)
}
func (l LocalFS) Unlock(tid string) {
	l.mu.Unlock(tid)
}

func (l LocalFS) tid2path(category string, tid string) (string, error) {

	name := l.tid2name(category, tid)
	eid, err := l.tid2eid(category, tid)
	if err != nil {
		return "", fmt.Errorf("could not get eid from tid: %w", err)
	}
	return filepath.Join(l.root, l.eid2dir(category, eid), name), nil
}

func (l LocalFS) tid2name(category string, tid string) string {
	switch category {
	case canonical:

		eid, err := zid.From(tid)
		if err != nil {
			return fmt.Sprintf("%s.eml", tid)
		}
		return fmt.Sprintf("%s.eml", eid.String())
	case log:
		return fmt.Sprintf("%s.log.jsonl", tid)
	}
	return fmt.Sprintf("%s.job", tid)
}

func (l LocalFS) tid2eid(category string, tid string) (zid.ID, error) {
	eid, _, err := zid.FromTID(tid)

	if err != nil && category == canonical {
		eid, err = zid.FromString(tid)
	}

	if err != nil {
		return zid.ID{}, fmt.Errorf("could not parse xid from tid: %w", err)
	}
	return eid, nil
}

func (l LocalFS) eid2dir(catalog string, eid zid.ID) string {
	t := eid.Time()
	day := t.In(time.UTC).Format("2006-01-02")
	hour := t.In(time.UTC).Format("15")
	return filepath.Join(catalog, day, hour)
}

func (l LocalFS) Remove(category string, path string) error {
	return os.Remove(filepath.Join(l.root, category, path))
}

func (l LocalFS) Walk(category string, fn func(tid string)) {
	dir := filepath.Join(l.root, category)
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		p, _, _ := strings.Cut(filepath.Base(path), ".") // TODO check if tid... if not return
		fn(p)
		return nil
	})
}

func (l LocalFS) Move(tid string, fromCategory string, toCategory string) error {
	name := l.tid2name(fromCategory, tid)
	eid, err := l.tid2eid(fromCategory, tid)
	if err != nil {
		return fmt.Errorf("could not get eid from tid: %w", err)
	}
	from := filepath.Join(l.root, l.eid2dir(fromCategory, eid), name)
	to := filepath.Join(l.root, l.eid2dir(toCategory, eid), name)

	err = os.MkdirAll(filepath.Dir(to), 0755)
	if err != nil {
		return err
	}
	return os.Rename(from, to)
}

func (l LocalFS) OpenReader(category string, tid string) (io.ReadCloser, error) {

	path, err := l.tid2path(category, tid)
	if err != nil {
		return nil, fmt.Errorf("could not get path from tid: %w", err)
	}

	return os.Open(path)
}

func (l LocalFS) OpenWriter(category string, tid string) (io.WriteCloser, error) {

	path, err := l.tid2path(category, tid)
	if err != nil {
		return nil, fmt.Errorf("could not get path from tid: %w", err)
	}

	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	if category == log {
		return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
	}

	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
}

func (l LocalFS) Exist(category string, tid string) bool {

	path, err := l.tid2path(category, tid)

	if err != nil {
		return false
	}

	_, err = os.Stat(path)
	fmt.Println("PPP", path, err)
	return !errors.Is(err, os.ErrNotExist)
}
