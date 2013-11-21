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
	createTblsSql = []string{
		"CREATE TABLE IF NOT EXISTS objinfo (fid INTEGER,label TEXT,hash TEXT,modtime INTEGER);",
		"CREATE TABLE IF NOT EXISTS chunks (hash TEXT,data BLOB);",
		"CREATE TABLE IF NOT EXISTS objindex (fid INTEGER,chunkrow INTEGER);",
	}
	insertIdxEntrySql = "INSERT INTO objindex VALUES(?,?);"
	insertIdxInfoSql  = "INSERT INTO objinfo VALUES(?,?,?,?);"
	insertChunkSql    = "INSERT INTO chunks (rowid,hash,data) VALUES(?,?,?);"
	getMaxFidSql      = "SELECT MAX(fid) FROM objinfo;"
	getMaxChunkRowSql = "SELECT MAX(rowid) FROM chunks;"
	chunkExistsSql    = "SELECT EXISTS(SELECT hash FROM chunks WHERE hash = ?);"
)

// Handler implements the rbup.Handler interface for storing split files in a
// sql database. Do NOT reuse a handler for multiple objects/files.
type Handler struct {
	label  string
	fid    int
	db     *sql.DB
	index  []int // []chunkrow
	fullH  hash.Hash
	chunkH hash.Hash
	tx     *sql.Tx
}

// Create a new handler dumping data chunks and index info to db for an
// object/file identified by label.
func New(db *sql.DB, label string) (h *Handler, err error) {
	h.tx, err = db.Begin()

	for _, sql := range createTblsSql {
		_, err := h.tx.Exec(sql)
		if err != nil {
			return nil, err
		}
	}

	rows, err := db.Query(getMaxFidSql)
	if err != nil {
		return nil, err
	}
	var maxfid int
	for rows.Next() {
		if err := rows.Scan(&maxfid); err != nil {
			return nil, err
		}
		break
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &Handler{
		label:  label,
		fid:    maxfid + 1,
		db:     db,
		fullH:  sha1.New(),
		chunkH: sha1.New(),
	}, nil
}

// Close writes the chunk index to the database.
func (h *Handler) Close() (err error) {
	defer func() {
		if err != nil {
			err = h.tx.Rollback()
			return
		}
		err = h.tx.Commit()
	}()

	for _, rowid := range h.index {
		if _, err := h.tx.Exec(insertIdxEntrySql, h.fid, rowid); err != nil {
			return err
		}
	}

	sumText := fmt.Sprintf("sha1-%x", h.fullH.Sum(nil))
	_, err = h.tx.Exec(insertIdxInfoSql, h.fid, h.label, sumText, time.Now())
	return err
}

// Write stores chunk in a hash-named file in the archive's dst directory.
func (h *Handler) Write(chunk []byte) (n int, err error) {
	// get next chunk rowid
	var maxrow int
	h.db.Query(getMaxChunkRowSql)
	rows, err := h.db.Query(getMaxChunkRowSql)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		if err := rows.Scan(&maxrow); err != nil {
			return 0, err
		}
		break
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// get chunk hashsum
	h.chunkH.Reset()
	h.chunkH.Write(chunk)
	sumText := fmt.Sprintf("sha1-%x", h.chunkH.Sum(nil))

	// check and return if chunk already exists
	rows, err = h.db.Query(chunkExistsSql, sumText)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var exists int
		if err := rows.Scan(&exists); err != nil {
			return 0, err
		}
		if exists == 1 {
			h.index = append(h.index, maxrow+1)
			return len(chunk), nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// add chunk to db
	_, err = h.tx.Exec(insertChunkSql, maxrow+1, sumText, chunk)
	if err != nil {
		return 0, err
	}
	h.index = append(h.index, maxrow+1)
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
