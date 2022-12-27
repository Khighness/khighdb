package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-27

func TestMurmur128_Write(t *testing.T) {
	mur := NewMurmur128()
	err := mur.Write([]byte("KHighness"))
	assert.Nil(t, err)
}

func TestMurmur128_EncodeSum128(t *testing.T) {
	mur := NewMurmur128()
	_ = mur.Write([]byte("KHighness"))
	t.Log(mur.EncodeSum128())
}

func TestMurmur128_Reset(t *testing.T) {
	mur := NewMurmur128()
	sum1 := mur.EncodeSum128()
	err := mur.Write([]byte("KHighness"))
	assert.Nil(t, err)
	mur.Reset()
	sum2 := mur.EncodeSum128()
	assert.Equal(t, sum1, sum2)
}
