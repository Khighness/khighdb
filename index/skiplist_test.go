package index

import (
	"strconv"
	"testing"
)

// @Author KHighness
// @Update 2022-11-16

var skl *SkipList

func initSkipList() {
	skl = NewSkipList()
	if skl == nil {
		panic("new skl error")
	}
}

func TestSkipList_Put(t *testing.T) {
	initSkipList()

	type User struct {
		id   int64
		name string
	}
	skl.Put([]byte("1"), User{id: 1, name: "K1"})
	t.Log("size = ", skl.Size())
}

func TestSkipList_Get(t *testing.T) {
	initSkipList()

	for i := 0; i < 5; i++ {
		skl.Put([]byte("K"+strconv.Itoa(i+1)), []byte("V"+strconv.Itoa(i+1)))
	}
	for i := 0; i < 5; i++ {
		elem := skl.Get([]byte("K" + strconv.Itoa(i+1)))
		t.Logf("(%s, %v)", string(elem.key), string(elem.value.([]byte)))
	}
}

func TestSkipList_Exist(t *testing.T) {
	initSkipList()

	for i := 0; i < 5; i++ {
		skl.Put([]byte("K"+strconv.Itoa(i+1)), []byte("V"+strconv.Itoa(i+1)))
	}
	for i := 0; i < 10; i++ {
		t.Log(skl.Exist([]byte("K" + strconv.Itoa(i+1))))
	}
}

func TestSkipList_Remove(t *testing.T) {
	initSkipList()

	for i := 0; i < 5; i++ {
		skl.Put([]byte("K"+strconv.Itoa(i+1)), []byte("V"+strconv.Itoa(i+1)))
	}
	for i := 0; i < 5; i++ {
		elem := skl.Remove([]byte("K" + strconv.Itoa(i+1)))
		t.Logf("(%s, %v)", string(elem.key), string(elem.value.([]byte)))
		t.Log("now size = ", skl.Size())
	}
}

func TestSkipList_Foreach(t *testing.T) {
	initSkipList()

	for i := 0; i < 5; i++ {
		skl.Put([]byte("K"+strconv.Itoa(i+1)), []byte("V"+strconv.Itoa(i+1)))
	}
	showAll := func(e *Element) bool {
		t.Logf("<%s, %v>", string(e.key), e.value)
		return true
	}
	showOne := func(e *Element) bool {
		t.Logf("<%s, %v>", string(e.key), e.value)
		return false
	}
	skl.Foreach(showAll)
	skl.Foreach(showOne)
}

func TestSkipList_FindPrefix(t *testing.T) {
	initSkipList()

	skl.Put([]byte("aaa"), 111)
	skl.Put([]byte("aaabbb"), 222)
	skl.Put([]byte("abcbbb"), 333)
	skl.Put([]byte("bbbaaa"), 444)
	skl.Put([]byte("bbbccc"), 555)

	e1 := skl.FindPrefix([]byte("aaa"))
	t.Logf("%+v", e1.value)

	e2 := skl.FindPrefix([]byte("abc"))
	t.Logf("%+v", e2.value)

	e3 := skl.FindPrefix([]byte("bbb"))
	t.Logf("%+v", e3.value)
}
