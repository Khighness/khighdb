package util

import "strconv"

// @Author KHighness
// @Update 2022-12-25

// Float64ToStr converts type float64 to string.
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// StrToFloat64 converts type string to float64.
func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// StrToInt64 converts type string to int64.
func StrToInt64(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}

// StrToUint converts type string to uint64.
func StrToUint(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}
