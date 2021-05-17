package dumper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func ModifyCopy(src, dst, pattern, replace string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	input, err := os.ReadFile(src)
	if err != nil {
		return 0, err
	}

	output := strings.ReplaceAll(string(input), pattern, replace)

	destination, err := create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()

	nBytes, err := destination.Write([]byte(output))
	return int64(nBytes), err
}

func create(p string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0770); err != nil {
		return nil, err
	}
	return os.Create(p)
}
