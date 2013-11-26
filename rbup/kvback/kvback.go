
package kvback

import (
	"crypto/sha1"
	"hash"
	"io"
	"path"
	"time"
	"errors"
	"encoding/json"

	"github.com/cznic/kv"
)

const HashName = "sha1"

type File struct {
	db *kv.DB
	Label string
	ObjId []byte // hash of object
	ModTime time.Time
	PrevVer []byte // hash of file
	Index [][]byte // list of chunk ids
}

func NewFile(db *kv.DB, label string) *File {
	return &File{
		db: db,
		Label: label,
		ModTime: time.Now(),
	}
}

func GetFile(db *kv.DB, fid []byte) (*File, error) {
	fdata, err := db.Get(nil, fid)
	if err != nil {
		return nil, err
	}
	if fdata == nil {
		return nil, errors.New("kvback: file does not exist")
	}

	f := &File{db: db}
	if err := json.Unmarshal(fdata, &f); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) AddChunk(id []byte) {
	f.Index = append(f.Index, id)
}

func (f *File) Reader() io.Reader {
	return &reader{
		index: f.Index,
		db: f.db,
	}
}

type reader struct {
	index [][]byte
	db *kv.DB
	i int
	buf []byte
}

func (r *reader) Read(data []byte) (n int, err error) {
	if r.i == len(r.index) {
		return 0, io.EOF
	}
	if len(r.buf) == 0 {
		r.buf, err = r.db.Get(nil, r.index[r.i])
		if err != nil {
			return 0, err
		}
		r.i++
	}
	n = copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (f *File) Prev() (*File, error) {
	if f.PrevVer == nil {
		return nil, errors.New("kvback: file has no previous version")
	}
	return GetFile(f.db, f.PrevVer)
}

func (f *File) Id() []byte {
	h := sha1.New()
	h.Write(f.Meta())
	return h.Sum(nil)
}
func (f *File) Meta() []byte {
	data, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	return data
}

const HandlePrefix = "handles"

type Handle struct {
	db *kv.DB
	f *File
	key []byte
}

func NewHandle(db *kv.DB, label string) (h *Handle, err error) {
	h = &Handle{
		db: db,
		key: []byte(path.Join(HandlePrefix, label)),
	}

	fid, err := db.Get(nil, h.key)
	if err == nil && fid != nil {
		h.f, err = GetFile(db, fid)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (h *Handle) Head() (*File, error) {
	if h.f == nil {
		return nil, errors.New("kvback: handle has no associated files")
	}
	return h.f, nil
}

func (h *Handle) Update(f *File) error {
	if h.f != nil {
		f.PrevVer = h.f.Id() // chain version history
	}
	h.f = f

	if err := h.db.Set(h.f.Id(), h.f.Meta()); err != nil {
		return err
	}
	if err := h.db.Set(h.key, h.f.Id()); err != nil {
		return err
	}
	return nil
}

// Handler implements the rbup.Handler interface for storing split files in a
// kv database.
type Handler struct {
	h        *Handle
	f *File
	fullH        hash.Hash
	chunkH       hash.Hash
	db           *kv.DB
}

// Create a new handler dumping data chunks and index info to db for an
// object/file identified by label.
func New(db *kv.DB, label string) (h *Handler, err error) {
	// config and return handler
	h = &Handler{db: db, fullH: sha1.New(), chunkH: sha1.New()}
	h.h, err = NewHandle(db, label)
	if err != nil {
		return nil, err
	}
	h.f = NewFile(db, label)
	return h, nil
}

// Close writes the chunk index to the database.
func (h *Handler) Close() error {
	if err := h.h.Update(h.f); err != nil {
		return err
	}
	return nil
}

// Write stores chunk in a hash-named file in the archive's dst directory.
func (h *Handler) Write(chunk []byte) (n int, err error) {
	// get chunk hashsum
	h.chunkH.Reset()
	h.chunkH.Write(chunk)
	h.fullH.Write(chunk)

	key := h.chunkH.Sum(nil)
	h.f.AddChunk(key)

	_, _, err = h.db.Put(nil, key, func(key, old []byte) (new []byte, write bool, err error) {
		if old == nil {
			return chunk, true, nil
		}
		return nil, false, nil
	})
	if err != nil {
		return 0, err
	}
	return len(chunk), nil
}

