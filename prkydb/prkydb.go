package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"runtime/debug"
	"time"

	"github.com/jamesdrando/prky"
)

// Work in progress terminal-based db client

func main() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			println(dep.Path, dep.Version)
		}
	}

	// The db path is  **required**
	path := os.Args[1]
	if !fs.ValidPath(path) {
		panic("Path is required. Run 'prky <path>'")
	}
	compactionInterval := flag.Int("compaction_interval", 1000, "Compaction interval")
	flag.Parse()

	var s *prky.Store
	var err error

	s, err = prky.NewStore(path, time.Duration(*compactionInterval)*time.Millisecond)

	if err != nil {
		panic(err)
	}

	var nwords int
	var command string
	var key string
	var value []byte
	var result []byte
	var version int64

	version, _, _ = s.Get("THX1137-42069")
	for {
		version, _, _ = s.Get("THX1137-42069")
		key = ""
		value = []byte{}
		err = nil

		fmt.Printf("prky<v>: %d\n", version)
		fmt.Print("prky<i>: ")
		nwords, err = fmt.Scanln(&command, &key, &value)
		// fmt.Println(nwords)

		if err != nil && err.Error() != "unexpected newline" {
			fmt.Println("prky<e>:", err)
			continue
		}

		if nwords < 2 || nwords > 3 {
			fmt.Println("prky<e>: Invalid number of arguments.")
			continue
		}

		switch command {
		case "get":
			version, result, err = s.Get(key)
			if err != nil {
				fmt.Println("prky<o>: Key not found.")
				continue
			} else {
				fmt.Println("prky<o>:", string(result))
			}
		case "put":
			version, _, err := s.Get(key)
			if err != nil {
				version = 0
			}
			err = s.Put(key, value, version)
			if err != nil {
				fmt.Printf("prky<e>: Failed adding key: %s\n", key)
			} else {
				fmt.Println("prky<o>: Added key-value pair.")
			}

		default:
			fmt.Println("prky<e>: Not implemented.")
		}

	}
}
