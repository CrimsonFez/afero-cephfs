package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/abiosoft/ishell"
	cephfs "github.com/crimsonfez/afero-cephfs"
	"github.com/spf13/afero"
)

func main() {

	var fsBackend string

	flag.StringVar(&fsBackend, "backend", "cephfs", "afero backend handler to use")

	flag.Parse()

	var fs afero.Fs

	switch fsBackend {
	case "cephfs":
		mount, err := cephfs.NewCephFS()
		if err != nil {
			fmt.Println(fmt.Errorf("failed to create cephfs mount: %v", err))
			return
		}

		fs = mount
		defer mount.Unmount()
	case "memfs":
		fs = afero.NewMemMapFs()
	default:
		fmt.Println(fmt.Errorf("invalid backend selected"))
		return
	}

	shell := ishell.New()
	shell.Println("Interactive Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "list",
		Func: func(c *ishell.Context) {
			path := "/"
			count := 0

			if len(c.Args) > 0 {
				path = c.Args[0]
			}

			if len(c.Args) > 1 {
				n, err := strconv.Atoi(c.Args[1])
				if err != nil {
					c.Err(fmt.Errorf("failed to parse string as int: %w", err))
					return
				}
				count = n
			}

			stat, err := fs.Stat(path)
			if err != nil {
				c.Err(fmt.Errorf("failed to access file at %s: %v", path, err))
				return
			}

			if stat.IsDir() {
				dir, err := fs.Open(path)
				if err != nil {
					c.Err(fmt.Errorf("failed to open %s: %v", path, err))
					return
				}
				defer dir.Close()

				list, err := dir.Readdirnames(count)
				for _, item := range list {
					c.Println(item)
				}
				if err != nil {
					c.Err(fmt.Errorf("failed to read directory: %v", err))
					return
				}

				return
			}

			c.Println(stat.Name())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "create",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 0 {
				c.Err(fmt.Errorf("you must provide a file path or name"))
				return
			}
			if len(c.Args) > 1 {
				c.Err(fmt.Errorf("multiple inputs provided, we only expect one"))
				return
			}

			path := c.Args[0]

			file, err := fs.Create(path)
			if err != nil {
				c.Err(fmt.Errorf("failed to create file: %v", err))
			}
			c.Println("file created")
			file.Close()
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "mkdir",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 0 {
				c.Err(fmt.Errorf("you must provide a file path"))
				return
			}
			if len(c.Args) > 1 {
				c.Err(fmt.Errorf("multiple inputs provided, we only expect one"))
				return
			}

			path := c.Args[0]

			err := fs.Mkdir(path, 0755)
			if err != nil {
				c.Err(fmt.Errorf("failed to create directory: %v", err))
				return
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "stat",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 0 {
				c.Err(fmt.Errorf("you must provide a file path"))
				return
			}
			if len(c.Args) > 1 {
				c.Err(fmt.Errorf("multiple inputs provided, we only expect one"))
				return
			}

			path := c.Args[0]

			info, err := fs.Stat(path)
			if err != nil {
				c.Err(fmt.Errorf("failed to open file: %v", err))
				return
			}
			jinfo, err := json.MarshalIndent(struct {
				Name  string      `json:"name"`
				Size  int64       `json:"size"`
				Mode  os.FileMode `json:"mode"`
				IsDir bool        `json:"isDir"`
			}{
				Name:  info.Name(),
				Size:  info.Size(),
				Mode:  info.Mode(),
				IsDir: info.IsDir(),
			}, "", "  ")
			if err != nil {
				c.Err(fmt.Errorf("failed to marshal file info: %v", err))
				return
			}
			c.Println(string(jinfo))
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "rm",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 0 {
				c.Err(fmt.Errorf("you must provide a file path"))
				return
			}
			if len(c.Args) > 1 {
				c.Err(fmt.Errorf("multiple inputs provided, we only expect one"))
				return
			}

			path := c.Args[0]

			if err := fs.Remove(path); err != nil {
				c.Err(fmt.Errorf("failed to remove file: %w", err))
			}
		},
	})

	shell.Run()
}
