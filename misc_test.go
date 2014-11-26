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
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/dataence/assert"
	"github.com/dataence/glog"
)

func fillLockFreeBuffer(t *testing.T, bufsize, ringsize int64) RingBuffer {
	buf, err := NewLockFreeBuffer(ringsize)

	assert.NoError(t, true, err)

	fillBuffer(t, buf, bufsize)

	assert.Equal(t, true, bufsize, buf.Len())

	return buf
}

func fillLockBuffer(t *testing.T, bufsize, ringsize int64) RingBuffer {
	buf, err := NewLockBuffer(ringsize)

	assert.NoError(t, true, err)

	fillBuffer(t, buf, bufsize)

	assert.Equal(t, true, bufsize, buf.Len())

	return buf
}

func fillBuffer(t *testing.T, buf RingBuffer, bufsize int64) {
	p := make([]byte, bufsize)
	for i := range p {
		p[i] = 'a'
	}

	n, err := buf.ReadFrom(bytes.NewBuffer(p))

	assert.Equal(t, true, bufsize, n)
	assert.Equal(t, true, err, io.EOF)
}

func peekBuffer(t *testing.T, buf RingBuffer, n int) {
	pkbuf, err := buf.Peek(n)

	assert.NoError(t, true, err)
	assert.Equal(t, true, n, len(pkbuf))

	for _, b := range pkbuf {
		assert.Equal(t, true, 'a', b)
	}
}

func testPeekCommit(t *testing.T, buf RingBuffer) {
	n := 10000

	go func(n int64) {
		fillBuffer(t, buf, n)
	}(int64(n))

	i := 0

	for n > 0 {
		pkbuf, _ := buf.Peek(1024)
		l, err := buf.Commit(len(pkbuf))

		assert.NoError(t, true, err)

		n -= l

		glog.Debugf("Commited %d bytes with %d remaining", l, n)

		i += l
	}
}

func testWriteTo(t *testing.T, buf RingBuffer) {
	n := int64(10000)

	go func(n int64) {
		fillBuffer(t, buf, n)
		time.Sleep(time.Millisecond * 100)
		buf.Close()
	}(n)

	m, err := buf.WriteTo(bytes.NewBuffer(make([]byte, n)))

	assert.Equal(t, true, io.EOF, err)
	assert.Equal(t, true, 10000, m)
}

func testRead(t *testing.T, buf RingBuffer) {
	n := int64(10000)

	go func(n int64) {
		fillBuffer(t, buf, n)
	}(n)

	p := make([]byte, n)
	i := 0

	for n > 0 {
		//glog.Debugf("n = %d", n)
		l, err := buf.Read(p[i:])

		assert.NoError(t, true, err)

		n -= int64(l)
		i += l
	}
}

func testCommit(t *testing.T, buf RingBuffer) {
	n, err := buf.Commit(256)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 256, n)

	_, err = buf.Commit(2048)

	assert.Equal(t, true, ErrBufferInsufficientData, err)
}

func testReadBytes(t *testing.T, buf RingBuffer) {
	p := make([]byte, 256)
	n, err := buf.Read(p)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 256, n)

	p2 := make([]byte, 4096)
	n, err = buf.Read(p2)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2048-256, n)
}

func benchmarkRead(b *testing.B, buf RingBuffer) {
	n := int64(b.N)

	go func(n int64) {
		p := make([]byte, n)
		buf.ReadFrom(bytes.NewBuffer(p))
	}(n)

	p := make([]byte, n)
	i := 0

	for n > 0 {
		//glog.Debugf("n = %d", n)
		l, _ := buf.Read(p[i:])

		n -= int64(l)
		i += l
	}
}
