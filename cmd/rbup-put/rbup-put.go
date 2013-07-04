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
	defer f.Close()

	a, err := rbup.NewArchiver(filepath.Base(fpath), dst)
	if err != nil {
		log.Fatal(err)
	}

	if err := rbup.Split(f, a); err != nil {
		log.Fatal(err)
	}
}
