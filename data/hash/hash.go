package hash

// @Author KHighness
// @Update 2022-11-27

// Hash defines the structure of hash table.
type Hash struct {
	record Record
}

// Record saves hash records.
type Record map[string]map[string][]byte

// New creates a new hash data structure.
func New() *Hash {
	return &Hash{record: make(Record)}
}

// HSet sets field in the hash stored at key to value.
// If key does not exist, a new key holding a hash will be created.
// If filed already exists in the hash, the value will be updated.
func (h *Hash) HSet(key string, filed string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if h.record[key][filed] != nil {
		h.record[key][filed] = value
	} else {
		h.record[key][filed] = value
		res = 1
	}
	return
}

// HSetNX sets field in the hash stored at key to value, only if field does not exist.
// If key does not exist, a new key holding a hash will be created
func (h *Hash) HSetNX(key string, filed string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	if _, exist := h.record[key][filed]; !exist {
		h.record[key][filed] = value
		return 1
	}
	return 0
}

// HGet returns the value associated with field in the hash stored at key.
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name if followed by its value,
// so the length of the reply is twice the size of the hash.
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

// HDel removes the specified fields from the hash stored at key. Specified fields that
// do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty and this command returns false.
func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

// HKeyExists returns is key exists in hash.
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// HExists returns if field is an existing field in the hash stored at key.
func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		ok = true
	}
	return
}

// HLen returns the number of fields contained in the hash stored at key.
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

// HKeys returns all filed names in the hash stored at key.
func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}

	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

// HValues returns all values in the hash stored at key.
func (h *Hash) HValues(key string) (val [][]byte) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

// HClear clears the key in the hash.
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

// exist checks if a key exists in the hash,
func (h *Hash) exist(key string) bool {
	_, ok := h.record[key]
	return ok
}
