package main

import (
	"io/ioutil"
	"os"
)

// @Author KHighness
// @Update 2022-12-25

func banner() string {
	file, err := os.Open("assets/banner.txt")
	if err != nil {
		panic(err)
	}
	buf, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
