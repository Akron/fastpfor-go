//go:build avogen
// +build avogen

package main

import (
	"flag"
	"strings"

	. "github.com/mmcloughlin/avo/build"
)

var (
	component = flag.String("component", "all", "component to generate")
)

// main emits both the delta and zigzag kernels so go:generate stays simple.
func main() {
	flag.Parse()

	comp := strings.ToLower(*component)

	Package("github.com/Akron/fastpfor-go")
	ConstraintExpr("amd64")
	ConstraintExpr("!noasm")

	if comp == "delta" || comp == "all" {
		genDeltaEncodeKernel()
		genDeltaDecodeKernel()
		genDeltaDecodeWithOverflowKernel()
	}

	if comp == "zigzag" || comp == "all" {
		genZigZagEncodeKernel()
		genZigZagDecodeKernel()
	}

	Generate()
}
