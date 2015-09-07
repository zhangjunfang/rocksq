package rocksq

import (
	rocks "github.com/tecbot/gorocksdb"
)

func makeSlice(s *rocks.Slice) []byte {
	slice := make([]byte, s.Size())
	copy(slice, s.Data())
	s.Free()
	return slice
}
