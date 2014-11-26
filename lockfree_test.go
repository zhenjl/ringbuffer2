// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ringbuffer2

import (
	"testing"

	"github.com/dataence/assert"
)

func TestLockFreeBufferReadFrom(t *testing.T) {
	fillLockFreeBuffer(t, 144, 4096)
	fillLockFreeBuffer(t, 2048, 4096)
	fillLockFreeBuffer(t, 3072, 4096)
}

func TestLockFreeBufferReadBytes(t *testing.T) {
	buf := fillLockFreeBuffer(t, 2048, 4096)

	testReadBytes(t, buf)
}

func TestLockFreeBufferCommitBytes(t *testing.T) {
	buf := fillLockFreeBuffer(t, 2048, 4096)

	testCommit(t, buf)
}

func TestLockFreeBufferConsumerProducerRead(t *testing.T) {
	buf, err := NewLockFreeBuffer(4096)

	assert.NoError(t, true, err)

	testRead(t, buf)
}

func TestLockFreeBufferConsumerProducerWriteTo(t *testing.T) {
	buf, err := NewLockFreeBuffer(4096)

	assert.NoError(t, true, err)

	testWriteTo(t, buf)
}

func TestLockFreeBufferConsumerProducerPeekCommit(t *testing.T) {
	buf, err := NewLockFreeBuffer(4096)

	assert.NoError(t, true, err)

	testPeekCommit(t, buf)
}

func TestLockFreeBufferPeek(t *testing.T) {
	lfbuf := fillLockFreeBuffer(t, 2048, 4096)

	peekBuffer(t, lfbuf, 100)
	peekBuffer(t, lfbuf, 1000)
}

func BenchmarkLockFreeBufferConsumerProducerRead(b *testing.B) {
	buf, _ := NewLockFreeBuffer(0)
	benchmarkRead(b, buf)
}
