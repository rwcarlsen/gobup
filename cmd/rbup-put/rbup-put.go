package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/rwcarlsen/gobup/rbup"
)

func main() {
	flag.Parse()
	fpath := flag.Arg(0)
	dst := flag.Arg(1)

	f, err := os.Open(fpath)
	if err != nil {
		log.Fatal(err)
	}

	ch := make(chan []byte)
	go func() {
		if err := rbup.Split(f, ch); err != nil {
			log.Fatal(err)
		}
	}()

	if err := rbup.Archive(ch, dst, filepath.Base(fpath)); err != nil {
		log.Fatal(err)
	}
}
