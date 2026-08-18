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

package clickhouse

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestImportPhaseCompletionRecorder(t *testing.T) {
	Convey("timed import phases call the completion recorder only after doing work", t, func() {
		var events []time.Duration

		err := timeImportPhase(func(phase string, duration time.Duration) {
			So(phase, ShouldEqual, "table_stage")

			events = append(events, duration)
		}, "table_stage", func() error {
			So(events, ShouldBeEmpty)

			return nil
		})

		So(err, ShouldBeNil)
		So(events, ShouldHaveLength, 1)
		So(events[0], ShouldBeGreaterThan, time.Duration(0))
	})
}
