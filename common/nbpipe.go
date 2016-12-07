/*
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package common

import (
	"container/list"
	"io"
)

type nbPipe struct {
	contents *list.List
}

type nbPipeWriter struct {
	pipe *nbPipe
}

type nbPipeReader struct {
	pipe *nbPipe
}

/*
MakeNBPipe returns a paired set of Reader and Writer objects.
Anything written to the reader will be available for consumption
by the writer.
There is no sychronization -- this is designed to be used in a single-threaded
context, such as inside a unit test.
*/
func MakeNBPipe() (io.ReadCloser, io.WriteCloser) {
	pipe := &nbPipe{
		contents: list.New(),
	}
	pr := &nbPipeReader{
		pipe: pipe,
	}
	pw := &nbPipeWriter{
		pipe: pipe,
	}
	return pr, pw
}

func (w *nbPipeWriter) Write(p []byte) (int, error) {
	w.pipe.contents.PushBack(p)
	return len(p), nil
}

func (w *nbPipeWriter) Close() error {
	w.pipe.contents.PushBack(nil)
	return nil
}

func (r *nbPipeReader) Read(p []byte) (int, error) {
	front := r.pipe.contents.Front()
	if front == nil {
		// Nothing to read yet
		return 0, nil
	}
	if front.Value == nil {
		// On EOF, leave value there so we keep picking it up
		return 0, io.EOF
	}

	val := front.Value.([]byte)
	copy(p, val)
	r.pipe.contents.Remove(front)
	if len(p) < len(val) {
		// Could not consume everything -- replace
		newVal := val[len(val)-len(p):]
		r.pipe.contents.PushFront(newVal)
		return len(p), nil
	}
	return len(val), nil
}

func (r *nbPipeReader) Close() error {
	return nil
}
