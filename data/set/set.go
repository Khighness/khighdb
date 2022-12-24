package set

// @Author KHighness
// @Update 2022-12-24

// Set defines the structure of set.
type Set struct {
	record Record
}

// Record saves set records.
type Record map[string]map[string]struct{}

// existFlag represents the key exists in the set.
var existFlag = struct{}{}

// New creates a new set data structure.
func New() *Set {
	return &Set{record: Record{}}
}

// SAdd adds the specified members to the set stored at key.
// Specified members that ar already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (s *Set) SAdd(key string, member []byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]struct{})
	}

	s.record[key][string(member)] = existFlag
	return len(s.record[key])
}

// exist checks if a key exists in the set.
func (s *Set) exist(key string) bool {
	_, ok := s.record[key]
	return ok
}
