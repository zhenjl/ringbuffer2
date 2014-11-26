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
	"bufio"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"

	"github.com/dataence/bithacks"
	"github.com/dataence/glog"
)

type LockFreeBuffer struct {
	id int32

	buf []byte
	tmp []byte

	size int64
	mask int64

	done int64

	pseq *sequence
	cseq *sequence

	cwait int64
	pwait int64
}

func NewLockFreeBuffer(size int64) (*LockFreeBuffer, error) {
	if size < 0 {
		return nil, bufio.ErrNegativeCount
	}

	if size == 0 {
		size = defaultBufferSize
	}

	if !bithacks.PowerOfTwo64(size) {
		return nil, fmt.Errorf("Size must be power of two. Try %d.", bithacks.RoundUpPowerOfTwo64(size))
	}

	if size < 2*defaultReadBlockSize {
		return nil, fmt.Errorf("Size must at least be %d. Try %d.", 2*defaultReadBlockSize, 2*defaultReadBlockSize)
	}

	return &LockFreeBuffer{
		id:    atomic.AddInt32(&bufcnt, 1),
		buf:   make([]byte, size),
		size:  size,
		mask:  size - 1,
		pseq:  newSequence(),
		cseq:  newSequence(),
		cwait: 0,
		pwait: 0,
	}, nil
}

func (this *LockFreeBuffer) ID() int32 {
	return this.id
}

func (this *LockFreeBuffer) Close() error {
	atomic.StoreInt64(&this.done, 1)
	return nil
}

func (this *LockFreeBuffer) Len() int {
	cpos := this.cseq.get()
	ppos := this.pseq.get()
	return int(ppos - cpos)
}

func (this *LockFreeBuffer) ReadFrom(r io.Reader) (int64, error) {
	total := int64(0)
	//p := make([]byte, defaultReadBlockSize)

	for {
		start, cnt, err := this.waitForWriteSpace(defaultReadBlockSize)
		if err != nil {
			return 0, err
		}

		pstart := int64(start) & this.mask
		pend := pstart + int64(cnt)
		if pend > int64(len(this.buf)) {
			pend = int64(len(this.buf))
		}

		//glog.Debugf("%d: got buffer at %d for %d bytes, %d bytes to buffer end", this.ID(), start, cnt, len(this.buf[pstart:]))

		n, err := r.Read(this.buf[pstart:pend])
		//glog.Debugf("%d: Read %d bytes", this.ID(), n)

		if n > 0 {
			this.pseq.set(start + int64(n))
			//m, err := this.Write(p[:n])
			//glog.Debugf("Wrote %d bytes", m)
			total += int64(n)

			//if err != nil {
			//	return total, err
			//}
		}

		if err != nil {
			//glog.Debugf("Error = %v", err)
			return total, err
		}
	}

	return total, nil
}

func (this *LockFreeBuffer) WriteTo(w io.Writer) (int64, error) {
	return writeTo(this, w)
}

func (this *LockFreeBuffer) Read(p []byte) (int, error) {
	pl := int64(len(p))

	// glog.Debugf("reading %d bytes", pl)

	for {
		cpos := this.cseq.get()
		ppos := this.pseq.get()
		cindex := cpos & this.mask

		//glog.Debugf("cpos = %d, ppos = %d, cindex = %d, len(p) = %d", cpos, ppos, cindex, pl)

		// If consumer position is at least len(p) less than producer position, that means
		// we have enough data to fill p. There are two scenarios that could happen:
		// 1. cindex + len(p) < buffer size, in this case, we can just copy() data from
		//    buffer to p, and copy will just copy enough to fill p and stop.
		//    The number of bytes copied will be len(p).
		// 2. cindex + len(p) > buffer size, this means the data will wrap around to the
		//    the beginning of the buffer. In thise case, we can also just copy data from
		//    buffer to p, and copy will just copy until the end of the buffer and stop.
		//    The number of bytes will NOT be len(p) but less than that.
		if cpos+pl < ppos {
			n := copy(p, this.buf[cindex:])

			//glog.Debugf("copied %d bytes into p", n)

			this.cseq.set(cpos + int64(n))
			return n, nil
		}

		// If we got here, that means there's not len(p) data available, but there might
		// still be data.

		// If cpos < ppos, that means there's at least ppos-cpos bytes to read. Let's just
		// send that back for now.
		if cpos < ppos {
			// n bytes available
			b := ppos - cpos

			// bytes copied
			var n int

			// if cindex+n < size, that means we can copy all n bytes into p.
			// No wrapping in this case.
			if cindex+b < this.size {
				n = copy(p, this.buf[cindex:cindex+b])
			} else {
				// If cindex+n >= size, that means we can copy to the end of buffer
				n = copy(p, this.buf[cindex:])
			}

			//glog.Debugf("copied %d bytes into p", n)

			this.cseq.set(cpos + int64(n))
			return n, nil
		}

		// If we got here, that means cpos >= ppos, which means there's no data available.
		// If so, let's wait...

		this.cwait++
		for ppos = this.pseq.get(); cpos >= ppos; ppos = this.pseq.get() {
			runtime.Gosched()

			if atomic.LoadInt64(&this.done) == 1 {
				return 0, io.EOF
			}
		}
	}

	return 0, nil
}

func (this *LockFreeBuffer) Write(p []byte) (int, error) {
	start, _, err := this.waitForWriteSpace(len(p))
	if err != nil {
		return 0, err
	}

	// If we are here that means we now have enough space to write the full p.
	// Let's copy from p into this.buf, starting at position ppos&this.mask.
	total := ringCopy(this.buf, p, int64(start)&this.mask)

	this.pseq.set(start + int64(len(p)))

	glog.Debugf("Wrote %d bytes", total)

	return total, nil
}

// Description below is copied completely from bufio.Peek()
//   http://golang.org/pkg/bufio/#Reader.Peek
// Peek returns the next n bytes without advancing the reader. The bytes stop being valid
// at the next read call. If Peek returns fewer than n bytes, it also returns an error
// explaining why the read is short. The error is bufio.ErrBufferFull if n is larger than
// b's buffer size.
// If there's not enough data to peek, error is ErrBufferInsufficientData.
// If n < 0, error is bufio.ErrNegativeCount
func (this *LockFreeBuffer) Peek(n int) ([]byte, error) {
	if int64(n) > this.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	//glog.Debugf("peeking %d bytes", n)

	cpos := this.cseq.get()
	ppos := this.pseq.get()

	// If there's no data, then let's wait until there is some data
	for ; cpos >= ppos; ppos = this.pseq.get() {
		runtime.Gosched()

		if atomic.LoadInt64(&this.done) == 1 {
			return nil, io.EOF
		}
	}

	// m = the number of bytes available. If m is more than what's requested (n),
	// then we make m = n, basically peek max n bytes
	m := ppos - cpos
	err := error(nil)

	if m >= int64(n) {
		m = int64(n)
	} else {
		err = ErrBufferInsufficientData
	}

	// There's data to peek. The size of the data could be <= n.
	if cpos+m <= ppos {
		cindex := cpos & this.mask

		// If cindex (index relative to buffer) + n is more than buffer size, that means
		// the data wrapped
		if cindex+m > this.size {
			// reset the tmp buffer
			this.tmp = this.tmp[0:0]

			l := len(this.buf[cindex:])
			this.tmp = append(this.tmp, this.buf[cindex:]...)
			this.tmp = append(this.tmp, this.buf[0:m-int64(l)]...)
			return this.tmp, err
		} else {
			return this.buf[cindex : cindex+m], err
		}
	}

	return nil, ErrBufferInsufficientData
}

// Commit moves the cursor forward by n bytes. It behaves like Read() except it doesn't
// return any data. If there's enough data, then the cursor will be moved forward and
// n will be returned. If there's not enough data, then the cursor will move forward
// as much as possible, then return the number of positions (bytes) moved.
func (this *LockFreeBuffer) Commit(n int) (int, error) {
	if int64(n) > this.size {
		return 0, bufio.ErrBufferFull
	}

	if n < 0 {
		return 0, bufio.ErrNegativeCount
	}

	cpos := this.cseq.get()
	ppos := this.pseq.get()

	//glog.Debugf("cpos = %d, ppos = %d, cindex = %d, n = %d", cpos, ppos, cindex, n)

	// If consumer position is at least n less than producer position, that means
	// we have enough data to fill p. There are two scenarios that could happen:
	// 1. cindex + n < buffer size, in this case, we can just copy() data from
	//    buffer to p, and copy will just copy enough to fill p and stop.
	//    The number of bytes copied will be len(p).
	// 2. cindex + n > buffer size, this means the data will wrap around to the
	//    the beginning of the buffer. In thise case, we can also just copy data from
	//    buffer to p, and copy will just copy until the end of the buffer and stop.
	//    The number of bytes will NOT be len(p) but less than that.
	if cpos+int64(n) <= ppos {
		//glog.Debugf("committing %d bytes", n)
		this.cseq.set(cpos + int64(n))
		return n, nil
	}

	return 0, ErrBufferInsufficientData
}

func (this *LockFreeBuffer) waitForWriteSpace(n int) (int64, int, error) {
	// The current producer position, remember it's a forever inreasing int64,
	// NOT the position relative to the buffer
	ppos := this.pseq.get()

	// The next producer position we will get to if we read the default block size
	next := ppos + int64(n)

	// For the producer, gate is the previous consumer sequence.
	gate := this.pseq.gate

	wrap := next - this.size

	//glog.Debugf("ppos = %d, next = %d, gate = %d, wrap = %d", ppos, next, gate, wrap)

	// If wrap point is greater than gate, that means the consumer hasn't read
	// some of the data in the buffer, and if we read in additional data and put
	// into the buffer, we would overwrite some of the unread data. It means we
	// cannot do anything until the customers have passed it. So we wait...
	//
	// Let's say size = 16, block = 4, ppos = 0, gate = 0
	//   then next = 4 (0+4), and wrap = -12 (4-16)
	//   _______________________________________________________________________
	//   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
	//   -----------------------------------------------------------------------
	//    ^                ^
	//    ppos,            next
	//    gate
	//
	// So wrap (-12) > gate (0) = false, and gate (0) > ppos (0) = false also,
	// so we move on (no waiting)
	//
	// Now if we get to ppos = 14, gate = 12,
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (12) = false, and gate (12) > ppos (14) = false aos,
	// so we move on again
	//
	// Now let's say we have ppos = 14, gate = 0 still (nothing read),
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (0) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 2 bytes
	// that are currently unread.
	//
	// Another scenario, let's say ppos = 100, gate = 80,
	// then next = 104 (100+4) and wrap = 88 (104-16)
	//
	// So wrap (88) > gate (80) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 8 bytes
	// that are currently unread.
	//
	if wrap > gate || gate > ppos {
		var cpos int64

		//glog.Debugf("cpos = %d", cpos)
		this.pwait++

		for cpos = this.cseq.get(); wrap > cpos; cpos = this.cseq.get() {
			runtime.Gosched()

			if atomic.LoadInt64(&this.done) == 1 {
				return 0, 0, io.EOF
			}
		}

		this.pseq.gate = cpos
	}

	return ppos, n, nil
}
