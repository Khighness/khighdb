package assets

import (
	"io/ioutil"
	"path/filepath"
)

// @Author KHighness
// @Update 2022-12-25

func banner() (string, error) {
	path, err := filepath.Abs("assets/banner.txt")
	if err != nil {
		return "", nil
	}
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return "", nil
	}
	return string(buf), nil
}
