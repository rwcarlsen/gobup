package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	_ "code.google.com/p/go-sqlite/go1/sqlite3"
	"github.com/rwcarlsen/gobup/rbup"
	"github.com/rwcarlsen/gobup/rbup/sqlback"
)

var dbpath = flag.String("db", filepath.Join(os.Getenv("HOME"), ".rbup.sqlite"), "database to dump data to")
var cpuprofile = flag.String("prof", "", "write cpu profile to file")

func main() {
	log.SetFlags(0)
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db, err := sql.Open("sqlite3", *dbpath)
	fatalif(err)
	defer db.Close()
	fatalif(sqlback.InitDB(db))

	for _, fname := range flag.Args() {
		fpath, err := filepath.Abs(fname)
		fatalif(err)
		f, err := os.Open(fpath)
		fatalif(err)

		info, err := sqlback.GetHeader(db, f)
		fatalif(err)
		if info != nil {
			if info.Label != fpath {
				info.Label = fpath
				info.ModTime = time.Now()
				fatalif(sqlback.PutHeader(db, info))
			}
		} else {
			h, err := sqlback.New(db, fpath)
			fatalif(err)

			_, err = f.Seek(0, os.SEEK_SET)
			fatalif(err)
			if err := rbup.Split(f, h); err != nil {
				log.Fatal(err)
			}
		}
		fatalif(f.Close())
	}
}

func fatalif(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
