// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import "runtime"

// curGoID returns the ID of the current goroutine by parsing the header of
// its own stack trace, which looks like "goroutine 123 [running]:".
//
// This is only invoked on the cold path where the pool has run out of
// capacity and a submitter is about to block, so the parsing cost is
// negligible compared to the ensuing synchronization.
func curGoID() int64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	b := buf[:n]

	// Skip the leading "goroutine " prefix.
	const prefix = len("goroutine ")
	if n <= prefix {
		return -1
	}
	b = b[prefix:]

	var id int64
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + int64(c-'0')
	}
	if id <= 0 {
		return -1
	}
	return id
}
