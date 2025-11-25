//go:build !amd64 || purego

package fastpfor

func simdPack(_ []byte, _ []uint32, _ int) bool {
	return false
}

func simdUnpack(_ []uint32, _ []byte, _, _ int) bool {
	return false
}

