//go:build !amd64 || noasm

package fastpfor

func initSIMDSelection() {}

func simdPack(_ []byte, _ []uint32, _ int) bool {
	return false
}

func simdUnpack(_ []uint32, _ []byte, _, _ int) bool {
	return false
}
