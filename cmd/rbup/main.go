package main

import (
	"flag"
	"path"
	"io"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"

	"github.com/rwcarlsen/gobup/rbup"
	"github.com/rwcarlsen/gobup/rbup/kvback"
	"github.com/cznic/kv"
)

var dbpath = flag.String("db", filepath.Join(os.Getenv("HOME"), ".rbup.kv"), "database to dump data to")
var list = flag.Bool("list", false, "list all backups starting with given prefix")
var cpuprofile = flag.String("prof", "", "write cpu profile to file")

func main() {
	defer func(){
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()
	log.SetFlags(0)
	flag.Parse()
	rbup.BlockSize = 1024 * 4

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db, err := kv.Create(*dbpath, &kv.Options{})
	if err != nil {
		db, err = kv.Open(*dbpath, &kv.Options{})
		fatalif(err)
	}
	defer db.Close()

	err = db.BeginTransaction()
	fatalif(err)
	defer db.Commit()

	if *list {
		path := path.Join(kvback.HandlePrefix, flag.Arg(0))
		enum, _, _ := db.Seek([]byte(path))
		for {
			key, _, err := enum.Next()
			if err == io.EOF {
				break
			}
			fatalif(err)

			if !bytes.HasPrefix(key, []byte(kvback.HandlePrefix)) {
				break
			}

			fmt.Printf("%s\n", key)
		}
		return
	}

	for _, fname := range flag.Args() {
		fpath, err := filepath.Abs(fname)
		fatalif(err)
		f, err := os.Open(fpath)
		fatalif(err)
		h, err := kvback.New(db, fpath)
		fatalif(err)
		fatalif(rbup.Split(f, h))
		fatalif(f.Close())
	}
}

func fatalif(err error) {
	if err != nil {
		panic(err)
	}
}
