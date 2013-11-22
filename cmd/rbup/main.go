package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"

	_ "code.google.com/p/go-sqlite/go1/sqlite3"
	"github.com/rwcarlsen/gobup/rbup"
	"github.com/rwcarlsen/gobup/rbup/sqlback"
)

var dbpath = flag.String("db", filepath.Join(os.Getenv("HOME"), ".rbup.sqlite"), "database to dump data to")

func main() {
	log.SetFlags(0)
	flag.Parse()

	db, err := sql.Open("sqlite3", *dbpath)
	fatalif(err)
	defer db.Close()

	fpath, err := filepath.Abs(flag.Arg(0))
	fatalif(err)

	h, err := sqlback.New(db, fpath)
	fatalif(err)

	f, err := os.Open(fpath)
	fatalif(err)
	defer f.Close()

	if err := rbup.Split(f, h); err != nil {
		log.Fatal(err)
	}
}

func fatalif(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
