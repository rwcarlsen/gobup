package kvback

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
	"path"

	"github.com/cznic/kv"
)

type Index [][]byte

func LoadIndex(db *kv.DB, indexId []byte) (Index, error) {
	data, err := ReadLarge(db, indexId)
	if err != nil {
		return nil, err
	}
	idx := make([][]byte, 0, len(data)/sha1.Size)
	for i := 0; i < len(data); i += sha1.Size {
		idx = append(idx, data[i:i+sha1.Size])
	}
	return idx, nil
}

func (idx *Index) AddObj(objid []byte) {
	*idx = append(*idx, objid)
}

func (idx Index) Save(db *kv.DB, key []byte) error {
	data := make([]byte, 0, sha1.Size*len(idx))
	for _, v := range idx {
		data = append(data, v...)
	}
	return WriteLarge(db, key, data)
}

func (idx Index) ObjReader(db *kv.DB) io.Reader {
	return &reader{
		idx: idx,
		db:  db,
	}
}

type reader struct {
	idx Index
	db  *kv.DB
	i   int
	buf []byte
}

func (r *reader) Read(data []byte) (n int, err error) {
	if r.i == len(r.idx) {
		return 0, io.EOF
	}
	if len(r.buf) == 0 {
		r.buf, err = r.db.Get(r.buf, r.idx[r.i])
		if err != nil {
			return 0, err
		}
		r.i++
	}
	n = copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

type Object []byte

func (o *Object) Id() []byte {
	h := sha1.New()
	h.Write(*o)
	return h.Sum(nil)
}

const maxSize = 65786

func ReadLarge(db *kv.DB, key []byte) (val []byte, err error) {
	enum, hit, err := db.Seek(key)
	if !hit {
		return nil, fmt.Errorf("kvback: key %x not found", key)
	} else if err != nil {
		return nil, err
	}

	if _, val, err = enum.Next(); err != nil && len(val) < maxSize {
		return val, nil // object is only a single val
	}
	var tot []byte
	for _, val, err = enum.Next(); err == nil; _, val, err = enum.Next() {
		tot = append(tot, val...)
		if len(val) < maxSize {
			return tot, nil
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return tot, nil
}

func WriteLarge(db *kv.DB, key, val []byte) (err error) {
	if len(val) <= maxSize {
		return db.Set(key, val)
	}

	exists := true
	_, _, err = db.Put(nil, key, func(key, old []byte) (new []byte, write bool, err error) {
		if old == nil {
			exists = false
			return val[:maxSize], true, nil
		}
		return nil, false, nil
	})
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	currkey := append(key, 1)
	i := 0
	for i = maxSize; i < len(val)-maxSize; i += maxSize {
		if err := db.Set(currkey, val[i:i+maxSize]); err != nil {
			return err
		}
		if currkey[len(currkey)-1] < 255 {
			currkey[len(currkey)-1] += 1
		} else {
			currkey = append(currkey, 1)
		}
	}
	return db.Set(currkey, val[i:])
}

const TagsPrefix = "tags"

// Handler implements the rbup.Handler interface for storing split files in a
// kv database.
type Handler struct {
	fullH  hash.Hash
	chunkH hash.Hash
	idx    Index
	db     *kv.DB
	tag    string
}

// Create a new handler dumping data chunks and index info to db for an
// object/file identified by label.
func New(db *kv.DB, tag string) *Handler {
	return &Handler{db: db, fullH: sha1.New(), chunkH: sha1.New(), tag: tag}
}

// Close writes the chunk index to the database.
func (h *Handler) Close() error {
	indexId := h.fullH.Sum(nil)
	if err := h.idx.Save(h.db, indexId); err != nil {
		return err
	}
	key := []byte(path.Join(TagsPrefix, h.tag))
	return h.db.Set(key, indexId)
}

// Write stores chunk in a hash-named file in the archive's dst directory.
func (h *Handler) Write(chunk []byte) (n int, err error) {
	h.chunkH.Reset()
	h.chunkH.Write(chunk)
	h.fullH.Write(chunk)

	objid := h.chunkH.Sum(nil)
	if err := WriteLarge(h.db, objid, chunk); err != nil {
		return 0, err
	}
	h.idx.AddObj(objid)
	return len(chunk), nil
}
