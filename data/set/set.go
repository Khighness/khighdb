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

// SPop removes and returns one or more random members from the set value stored at key.
func (s *Set) SPop(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count <= 0 {
		return val
	}

	for k := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))

		count--
		if count == 0 {
			break
		}
	}
	return val
}

// SIsMember returns if member is a member of the set stored at key.
func (s *Set) SIsMember(key string, member []byte) bool {
	return s.fieldExist(key, string(member))
}

// SRandMember when called with just the key argument, returns a random
// element from the set valued stored at key.
func (s *Set) SRandMember(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count == 0 {
		return val
	}

	if count > 0 {
		for k := range s.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return val
}

// SRem removes the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns false.
func (s *Set) SRem(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	if _, ok := s.record[key][string(member)]; ok {
		delete(s.record[key], string(member))
		return true
	}
	return false
}

// SMove moves member from the set at source to the set at destination.
// If the source set does not exist or does not contain the specified element,
// no operation is performed and return false.
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.fieldExist(src, string(member)) {
		return false
	}
	if !s.exist(dst) {
		s.record[dst] = make(map[string]struct{})
	}

	delete(s.record[src], string(member))
	s.record[dst][string(member)] = existFlag

	return true
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}

	return len(s.record[key])
}

// SMembers returns all the members of the set value stored at key.
func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}

	for k := range s.record[key] {
		val = append(val, []byte(k))
	}
	return
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (s *Set) SUnion(keys ...string) (val [][]byte) {
	m := make(map[string]bool)
	for _, k := range keys {
		if s.exist(k) {
			for v := range s.record[k] {
				m[v] = true
			}
		}
	}

	for v := range m {
		val = append(val, []byte(v))
	}
	return
}

// SDiff returns the members of the set resulting from the difference
// between the first set and all the successive sets.
func (s *Set) SDiff(keys ...string) (val [][]byte) {
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}

	for v := range s.record[keys[0]] {
		flag := true
		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

// SKeyExists returns if the key exists.
func (s *Set) SKeyExists(key string) (ok bool) {
	return s.exist(key)
}

// SClear clears the specified key in set
func (s *Set) SClear(key string) {
	if s.SKeyExists(key) {
		delete(s.record, key)
	}
}

// exist checks if a key exists in the set.
func (s *Set) exist(key string) bool {
	_, exist := s.record[key]
	return exist
}

// fieldExist checks if a filed of key exists in the set.
func (s *Set) fieldExist(key, field string) bool {
	fields, exist := s.record[key]
	if !exist {
		return false
	}
	_, ok := fields[field]
	return ok
}
