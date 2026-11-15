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

package ants_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/panjf2000/ants/v2"
)

// TestNestedSubmitSamePool reproduces the deadlock where outer tasks occupying
// all workers of a full pool submit subtasks to the same pool and wait for them:
// every worker is blocked in retrieveWorker() and no worker ever comes back,
// so the subtasks never get executed. After the fix, all subtasks must finish
// within the time limit.
func TestNestedSubmitSamePool(t *testing.T) {
	const (
		poolCap       = 2
		outers        = poolCap // outer tasks fill up the whole pool
		subsPerOuter  = 4
		totalSubTasks = outers * subsPerOuter // 8 subtasks in total
	)

	p, err := ants.NewPool(poolCap)
	require.NoError(t, err)
	defer p.Release()

	var executed int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		var outerWg sync.WaitGroup
		for i := 0; i < outers; i++ {
			outerWg.Add(1)
			require.NoError(t, p.Submit(func() {
				defer outerWg.Done()
				var innerWg sync.WaitGroup
				for j := 0; j < subsPerOuter; j++ {
					innerWg.Add(1)
					if err := p.Submit(func() {
						defer innerWg.Done()
						atomic.AddInt32(&executed, 1)
					}); err != nil {
						innerWg.Done()
					}
				}
				innerWg.Wait()
			}))
		}
		outerWg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("nested submits are stuck: %d/%d subtasks executed after 3s",
			atomic.LoadInt32(&executed), totalSubTasks)
	}
	require.EqualValues(t, totalSubTasks, atomic.LoadInt32(&executed))

	// The transient overflow workers must retire themselves after serving the
	// nested submissions, converging the pool back to its configured capacity.
	require.Eventually(t, func() bool {
		return p.Running() <= poolCap
	}, 3*time.Second, 10*time.Millisecond)
}

// TestNestedInvokeSamePool is the PoolWithFunc counterpart of
// TestNestedSubmitSamePool.
func TestNestedInvokeSamePool(t *testing.T) {
	const (
		poolCap       = 2
		outers        = poolCap
		subsPerOuter  = 4
		totalSubTasks = outers * subsPerOuter
	)

	var executed int32
	var subWg, outerWg sync.WaitGroup

	var p *ants.PoolWithFunc
	p, err := ants.NewPoolWithFunc(poolCap, func(arg any) {
		if idx := arg.(int); idx < 0 { // outer task: submit subtasks and wait for them
			defer outerWg.Done()
			for j := 0; j < subsPerOuter; j++ {
				subWg.Add(1)
				if err := p.Invoke(j); err != nil {
					subWg.Done()
				}
			}
			subWg.Wait()
			return
		}
		// subtask
		defer subWg.Done()
		atomic.AddInt32(&executed, 1)
	})
	require.NoError(t, err)
	defer p.Release()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < outers; i++ {
			outerWg.Add(1)
			require.NoError(t, p.Invoke(-i-1))
		}
		outerWg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("nested invokes are stuck: %d/%d subtasks executed after 3s",
			atomic.LoadInt32(&executed), totalSubTasks)
	}
	require.EqualValues(t, totalSubTasks, atomic.LoadInt32(&executed))
}

// TestNestedSubmitMultiLevel verifies that deeper nesting (subtasks submitting
// their own subtasks) also makes progress instead of deadlocking.
func TestNestedSubmitMultiLevel(t *testing.T) {
	const total = 1 + 2 + 4 // 1 root + 2 children + 4 grandchildren

	p, err := ants.NewPool(2)
	require.NoError(t, err)
	defer p.Release()

	var executed int32
	var wg sync.WaitGroup
	var submit func(depth int)
	submit = func(depth int) {
		atomic.AddInt32(&executed, 1)
		if depth == 2 {
			return
		}
		var inner sync.WaitGroup
		for i := 0; i < 2; i++ {
			inner.Add(1)
			wg.Add(1)
			if err := p.Submit(func() {
				defer wg.Done()
				defer inner.Done()
				submit(depth + 1)
			}); err != nil {
				inner.Done()
				wg.Done()
			}
		}
		inner.Wait()
	}

	wg.Add(1)
	require.NoError(t, p.Submit(func() {
		defer wg.Done()
		submit(0)
	}))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("multi-level nested submits are stuck: %d/%d tasks executed after 3s",
			atomic.LoadInt32(&executed), total)
	}
	require.EqualValues(t, total, atomic.LoadInt32(&executed))
}

// TestNestedSubmitNonblockingKeepsError verifies that a pool created with
// WithNonblocking(true) still returns ErrPoolOverload for a nested submission
// when the pool is full, i.e. the explicit non-blocking contract is untouched.
func TestNestedSubmitNonblockingKeepsError(t *testing.T) {
	p, err := ants.NewPool(1, ants.WithNonblocking(true))
	require.NoError(t, err)
	defer p.Release()

	nestedErr := make(chan error, 1)
	done := make(chan struct{})
	require.NoError(t, p.Submit(func() {
		defer close(done)
		// This runs on the pool's only worker while the pool is full:
		// a nested submit must keep the non-blocking contract and fail fast
		// with ErrPoolOverload instead of blocking forever.
		nestedErr <- p.Submit(func() {})
	}))

	select {
	case err := <-nestedErr:
		require.ErrorIs(t, err, ants.ErrPoolOverload)
	case <-time.After(3 * time.Second):
		t.Fatal("nested submit on a full non-blocking pool is stuck")
	}
	<-done
}
