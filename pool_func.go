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

// PoolWithFunc is like Pool but accepts a unified function for all goroutines to execute.
type PoolWithFunc struct {
	*poolCommon

	// fn is the unified function for processing tasks.
	fn func(any)
}

// Invoke passes arguments to the pool.
//
// It is allowed to call PoolWithFunc.Invoke() from inside a task running on
// the same pool (a nested invocation): when the pool has run out of capacity,
// such a nested invocation is served by a transient overflow worker instead of
// blocking, so the pool won't deadlock. The overflow worker retires itself
// once the task is done and the pool converges back to its capacity.
// Note that a pool instantiated with ants.WithNonblocking(true) keeps its
// fail-fast contract and returns ErrPoolOverload even for nested invocations.
func (p *PoolWithFunc) Invoke(arg any) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}

	w, err := p.retrieveWorker()
	if w != nil {
		w.inputArg(arg)
	}
	return err
}

// NewPoolWithFunc instantiates a PoolWithFunc with customized options.
func NewPoolWithFunc(size int, pf func(any), options ...Option) (*PoolWithFunc, error) {
	if pf == nil {
		return nil, ErrLackPoolFunc
	}

	pc, err := newPool(size, options...)
	if err != nil {
		return nil, err
	}

	pool := &PoolWithFunc{
		poolCommon: pc,
		fn:         pf,
	}

	pool.workerCache.New = func() any {
		return &goWorkerWithFunc{
			pool: pool,
			arg:  make(chan any, workerChanCap),
		}
	}

	return pool, nil
}
