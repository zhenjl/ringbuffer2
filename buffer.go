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
	"errors"
	"io"
)

var (
	bufcnt int32
)

type RingBuffer interface {
	ReadFrom(r io.Reader) (int64, error)
	WriteTo(w io.Writer) (int64, error)

	Read(p []byte) (int, error)
	Write(p []byte) (int, error)

	Peek(n int) ([]byte, error)
	Commit(n int) (int, error)

	Len() int
	ID() int32

	Close() error
}

const (
	defaultReadBlockSize  = 1024
	defaultWriteBlockSize = 2048
	defaultBufferSize     = 1024 * 1024 // 1 MB
)

var (
	ErrBufferInsufficientData error = errors.New("RingBuffer: Insufficient data.")
)

func readFrom(buf RingBuffer, r io.Reader) (int64, error) {
	total := int64(0)
	p := make([]byte, defaultReadBlockSize)

	for {
		n, err := r.Read(p)
		//glog.Debugf("%d: Read %d bytes", buf.ID(), n)

		if n > 0 {
			m, err := buf.Write(p[:n])
			//glog.Debugf("Wrote %d bytes", m)
			total += int64(m)

			if err != nil {
				return total, err
			}
		}

		if err != nil {
			//glog.Debugf("Error = %v", err)
			return total, err
		}
	}

	return total, nil
}

func writeTo(buf RingBuffer, w io.Writer) (int64, error) {
	total := int64(0)

	for {
		p, err := buf.Peek(defaultWriteBlockSize)

		// There's some data, let's process it first
		if len(p) > 0 {
			n, err := w.Write(p)
			total += int64(n)

			if err != nil {
				return total, err
			}

			buf.Commit(len(p))
			//glog.Debugf("%d: Wrote %d bytes", buf.ID(), len(p))
		}

		if err != ErrBufferInsufficientData && err != nil {
			//glog.Debugf("Error = %v", err)
			return total, err
		}
	}

	return total, nil
}

func ringCopy(dst, src []byte, start int64) int {
	n := len(src)

	i, l := 0, 0

	for n > 0 {
		l = copy(dst[start:], src[i:])
		i += l
		n -= l

		if n > 0 {
			start = 0
		}
	}

	return i
}
