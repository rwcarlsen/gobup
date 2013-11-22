// Package sqlback implements the github.com/rwcarlsen/rbup.Handler
// interface.
package sqlback

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"path/filepath"
	"time"
)

var (
	insertIdxEntrySql = "INSERT INTO objindex VALUES(?,?);"
	insertIdxInfoSql  = "INSERT INTO objinfo VALUES(?,?,?,?);"
	insertChunkSql    = "INSERT INTO chunks (rowid,hash,data) VALUES(?,?,?);"
	getMaxFidSql      = "SELECT MAX(fid) FROM objinfo;"
	getMaxChunkRowSql = "SELECT MAX(rowid) FROM chunks;"
	chunkRowSql       = "SELECT rowid FROM chunks WHERE hash = ?;"
)

func sumText(hashsum []byte) string {
	return fmt.Sprintf("sha1-%x", hashsum)
}

// Handler implements the rbup.Handler interface for storing split files in a
// sql database. Do NOT reuse a handler for multiple objects/files.
type Handler struct {
	label        string
	fid          int
	db           *sql.DB
	index        []int // []chunkrow
	fullH        hash.Hash
	chunkH       hash.Hash
	tx           *sql.Tx
	nextChunkRow int
}

// Create a new handler dumping data chunks and index info to db for an
// object/file identified by label.
func New(db *sql.DB, label string) (h *Handler, err error) {
	if err := InitDB(db); err != nil {
		return nil, err
	}

	// get next file/object id
	var maxfid sql.NullInt64
	row := db.QueryRow(getMaxFidSql)
	if err := row.Scan(&maxfid); err != nil {
		return nil, err
	}

	// get next chunk rowid
	var maxrow sql.NullInt64
	row = db.QueryRow(getMaxChunkRowSql)
	if err := row.Scan(&maxrow); err != nil {
		return nil, err
	}

	// config and return handler
	h = &Handler{}
	h.tx, err = db.Begin()
	if err != nil {
		return nil, err
	}
	h.nextChunkRow = int(maxrow.Int64) + 1
	h.label = label
	h.fid = int(maxfid.Int64) + 1
	h.db = db
	h.fullH = sha1.New()
	h.chunkH = sha1.New()
	return h, nil
}

// Close writes the chunk index to the database.
func (h *Handler) Close() (err error) {
	defer func() {
		if err != nil {
			h.tx.Rollback()
			return
		}
		err = h.tx.Commit()
	}()

	for _, rowid := range h.index {
		if _, err := h.tx.Exec(insertIdxEntrySql, h.fid, rowid); err != nil {
			return err
		}
	}

	sum := sumText(h.fullH.Sum(nil))
	_, err = h.tx.Exec(insertIdxInfoSql, h.fid, h.label, sum, time.Now())
	return err
}

// Write stores chunk in a hash-named file in the archive's dst directory.
func (h *Handler) Write(chunk []byte) (n int, err error) {
	// get chunk hashsum
	h.chunkH.Reset()
	h.chunkH.Write(chunk)
	h.fullH.Write(chunk)
	sum := sumText(h.chunkH.Sum(nil))

	// check and return rowid if chunk already exists
	row := h.tx.QueryRow(chunkRowSql, sum)
	var rowid int
	if err := row.Scan(&rowid); err == nil {
		h.index = append(h.index, rowid)
		return len(chunk), nil
	} else if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// add chunk to db
	_, err = h.tx.Exec(insertChunkSql, h.nextChunkRow, sum, chunk)
	if err != nil {
		return 0, err
	}
	h.index = append(h.index, h.nextChunkRow)
	h.nextChunkRow++
	return len(chunk), nil
}

// reader is a special io.Reader that gradually returns bytes from all the
// objects for a split file chunk by chunk.
type reader struct {
	dst      string
	indx     []string
	buf      []byte
	objIndex int
}

func (r *reader) Read(data []byte) (n int, err error) {
	if r.objIndex == len(r.indx) {
		return 0, io.EOF
	}

	if len(r.buf) == 0 {
		fpath := filepath.Join(r.dst, r.indx[r.objIndex])
		r.buf, err = ioutil.ReadFile(fpath)
		if err != nil {
			return 0, err
		}
		r.objIndex++
	}

	n = copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
