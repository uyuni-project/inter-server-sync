package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

const (
	usage = `ISS export tool

Usage: %s [Options]
	

Options:
`
)

type Cliargs struct {
	ChannleLabels []string
	Path          string
	Config        string
	Dot           bool
	Debug         bool
	Cpuprofile    string
	Memprofile    string
}

func CliArgs(args []string) (*Cliargs, error) {

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), usage, os.Args[0])
		flag.PrintDefaults()
	}
	channelLabels := flag.String("channels", "", "Labels for channels to sync (comma seprated in case of multiple)")

	path := flag.String("path", ".", "Location for generated data")

	config := flag.String("config", "/etc/rhn/rhn.conf", "Path for the config file")

	dot := flag.Bool("dot", false, "Create dot file for Graphviz")

	debug := flag.Bool("debug", false, "debug export data")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")

	if len(args) < 2 {
		flag.Usage()
		return nil, errors.New("Insufficent arguments")
	}

	flag.Parse()

	return &Cliargs{strings.Split(*channelLabels, ","), *path, *config, *dot, *debug, *cpuprofile, *memprofile}, nil
}
