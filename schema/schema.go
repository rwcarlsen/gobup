package schema

import (
	"crypto/sha1"

	"github.com/rwcarlsen/gobup/rbup"
)

type ChunkRef struct {
	ChunkId  string // hash of chunk content
	ObjectId string // object chunk is stored in
	Offset   int    // byte offset into Object
}

type Index []ChunkRef

type Backend interface {
	FindChunk(chunkId string) (*ChunkRef, error)
	AddObject(objid string, objdata io.Reader) error
	AddIndex(tag string, idx Index) error
}

type FileBack struct {
	Root string
}

func (fb *FileBack) FindChunk(chunkId string) (*ChunkRef, error) {
}
func (fb *FileBack) AddObject(objid string, objdata io.Reader) error {
	f, err := os.Create(objid)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err := io.Copy(f, objdata)
	if err != nil {
		return err
	}
	return nil
}
func (fb *FileBack) AddIndex(tag string, idx Index) error {
	f, err := os.Create(fmt.Printf("%v-%", tag, time.Now().Unix()))
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	_, err := f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

type Handler struct {
	tag  string
	buf  bytes.Buffer
	refs Index
	idx  Index
	back Backend
}

func New(tag string, back Backend) *Handler {
	return &Handler{
		tag: tag,
		back: back,
	}
}

func (h *Handler) Close() (err error) {
	if err := h.dump(); err != nil {
		return err
	}
	return h.back.AddIndex(h.tag, h.idx)
}

func (h *Handler) dump() error {
	objid := fmt.Sprintf("%x", h.fullH.Sum(nil))
	h.fullH.Reset()
	for _, ref := range h.refs {
		if ref.ObjectId != "" {
			ref.ObjectId = objid
		}
	}
	h.idx = append(h.idx, h.refs...)
	h.refs = h.refs[:0]
	if err := h.back.AddObject(objid, h.buf); err != nil {
		return err
	}
}

func (h *Handler) Write(chunk []byte) (n int, err error) {
	hs := sha1.New()
	hs.Write(chunk)
	chunkid := fmt.Sprintf("%x", hs.Sum(nil))

	if ref, err := h.back.FindChunk(chunkid); ref != nil && err != nil {
		h.refs = append(h.refs, ref)
	} else {
		ref := &ChunkRef{
			ChunkId: chunkid,
			Offset:  buf.Len(),
		}
		h.fullH.Write(chunk)
		_, err := h.buf.Write(chunk)
		if err != nil {
			return 0, err
		}
		h.refs = append(h.refs, ref)
	}

	h.refs = append(h.refs, ref)
	return len(chunk), nil
}
