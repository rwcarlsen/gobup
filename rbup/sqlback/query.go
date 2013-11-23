package sqlback

import (
	"crypto/sha1"
	"database/sql"
	"io"
	"time"
)

var (
	initSql = []string{
		"PRAGMA cache_size = 10000;",
		"PRAGMA page_size = 4096;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA synchronous = OFF;",
		"CREATE TABLE IF NOT EXISTS objinfo (fid INTEGER,label TEXT,hash TEXT,modtime INTEGER);",
		"CREATE TABLE IF NOT EXISTS chunks (hash TEXT,data BLOB);",
		"CREATE TABLE IF NOT EXISTS objindex (fid INTEGER,chunkrow INTEGER);",
		"CREATE INDEX IF NOT EXISTS chunks_hash ON chunks (hash ASC);",
		"CREATE INDEX IF NOT EXISTS objinfo_hash ON objinfo (hash ASC);",
		"CREATE INDEX IF NOT EXISTS objinfo_label ON objinfo (label ASC);",
	}
	objExistsSql = "SELECT fid,label,hash,modtime FROM objinfo WHERE hash = ?;"
)

type ObjHeader struct {
	Fid     int
	Label   string
	HashSum string
	ModTime time.Time
}

// InitDB creates database structure for storing chunked objects and indices.
func InitDB(db *sql.DB) error {
	// create tables
	for _, sql := range initSql {
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetHeader returns the object's summary info if the object already exists.
// Returns nil otherwise. r contains the bytes of the object to search for in
// db.
func GetHeader(tx *sql.Tx, r io.Reader) (info *ObjHeader, err error) {
	h := sha1.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, err
	}

	info = &ObjHeader{}
	row := tx.QueryRow(objExistsSql, sumText(h.Sum(nil)))
	var t int64
	err = row.Scan(&info.Fid, &info.Label, &info.HashSum, &t)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		info.ModTime = time.Unix(t, 0)
		return info, nil
	}
}

// PutHeader adds an object header to db.  This should only be called to
// create a new, updated header entry for an object that already exists.
func PutHeader(tx *sql.Tx, info *ObjHeader) (err error) {
	_, err = tx.Exec(insertIdxInfoSql, info.Fid, info.Label, info.HashSum, info.ModTime)
	return err
}
