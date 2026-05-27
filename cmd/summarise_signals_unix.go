//go:build !windows

/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package cmd

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func summariseSignalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGTERM:
		return "SIGTERM"
	case os.Interrupt:
		return "SIGINT"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	default:
		return sig.String()
	}
}

func resignalCurrentProcess(sig os.Signal) {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		os.Exit(1)
	}

	if err := p.Signal(sig); err != nil {
		os.Exit(1)
	}
}

func (d *summariseDiagnostics) startSignalHandler() {
	if d == nil {
		return
	}

	signals := []os.Signal{syscall.SIGTERM, os.Interrupt, syscall.SIGQUIT}
	ch := make(chan os.Signal, 1)
	done := make(chan struct{})

	var once sync.Once

	signal.Notify(ch, signals...)

	d.signalStop = func() {
		once.Do(func() {
			signal.Stop(ch)
			close(done)
		})
	}

	go func() {
		select {
		case sig := <-ch:
			d.logSignal(sig)
			signal.Reset(sig)
			resignalCurrentProcess(sig)
		case <-done:
		}
	}()
}

func (d *summariseDiagnostics) stopSignalHandler() {
	if d == nil || d.signalStop == nil {
		return
	}

	d.signalStop()
}
