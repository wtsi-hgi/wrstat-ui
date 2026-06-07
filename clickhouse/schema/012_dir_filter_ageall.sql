/* ******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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
 ***************************************************************************** */

CREATE TABLE IF NOT EXISTS wrstat_dir_filter_ageall (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  gid UInt32,
  uid UInt32,
  ft UInt16,
  dir String CODEC(LZ4),
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, gid, uid, ft, dir)
SETTINGS index_granularity = 8192;
