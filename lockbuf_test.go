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

func TestLockBufferReadFrom(t *testing.T) {
	fillLockBuffer(t, 144, 4096)
	fillLockBuffer(t, 2048, 4096)
	fillLockBuffer(t, 3072, 4096)
}

func TestLockBufferReadBytes(t *testing.T) {
	buf := fillLockBuffer(t, 2048, 4096)

	testReadBytes(t, buf)
}

func TestLockBufferCommitBytes(t *testing.T) {
	buf := fillLockBuffer(t, 2048, 4096)

	testCommit(t, buf)
}

func TestLockBufferConsumerProducerRead(t *testing.T) {
	buf, err := NewLockBuffer(4096)

	assert.NoError(t, true, err)

	testRead(t, buf)
}

func TestLockBufferConsumerProducerWriteTo(t *testing.T) {
	buf, err := NewLockBuffer(4096)

	assert.NoError(t, true, err)

	testWriteTo(t, buf)
}

func TestLockBufferConsumerProducerPeekCommit(t *testing.T) {
	buf, err := NewLockBuffer(4096)

	assert.NoError(t, true, err)

	testPeekCommit(t, buf)
}

func TestLockBufferPeek(t *testing.T) {
	buf := fillLockBuffer(t, 2048, 4096)

	peekBuffer(t, buf, 100)
	peekBuffer(t, buf, 1000)
}

func BenchmarkLockBufferConsumerProducerRead(b *testing.B) {
	buf, _ := NewLockBuffer(0)
	benchmarkRead(b, buf)
}
