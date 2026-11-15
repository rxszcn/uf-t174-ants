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

// Pool is a goroutine pool that limits and recycles a mass of goroutines.
// The pool capacity can be fixed or unlimited.
type Pool struct {
	*poolCommon
}

// Submit submits a task to the pool.
//
// It is allowed to call Pool.Submit() from inside a task running on the same
// pool (a nested submission): when the pool has run out of capacity, such a
// nested submission is served by a transient overflow worker instead of
// blocking, so the pool won't deadlock. The overflow worker retires itself
// once the task is done and the pool converges back to its capacity.
// Note that a pool instantiated with ants.WithNonblocking(true) keeps its
// fail-fast contract and returns ErrPoolOverload even for nested submissions.
func (p *Pool) Submit(task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}

	w, err := p.retrieveWorker()
	if w != nil {
		w.inputFunc(task)
	}
	return err
}

// NewPool instantiates a Pool with customized options.
func NewPool(size int, options ...Option) (*Pool, error) {
	pc, err := newPool(size, options...)
	if err != nil {
		return nil, err
	}

	pool := &Pool{poolCommon: pc}
	pool.workerCache.New = func() any {
		return &goWorker{
			pool: pool,
			task: make(chan func(), workerChanCap),
		}
	}

	return pool, nil
}
