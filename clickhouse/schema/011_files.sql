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

CREATE TABLE IF NOT EXISTS wrstat_files (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir_id UInt32 CODEC(Delta, LZ4),
  name String CODEC(LZ4),
  ext LowCardinality(String) CODEC(LZ4),
  entry_type UInt8,
  size UInt64 CODEC(Delta, LZ4),
  apparent_size UInt64 CODEC(Delta, LZ4),
  uid UInt32,
  gid UInt32,
  atime DateTime CODEC(Delta, LZ4),
  mtime DateTime CODEC(Delta, LZ4),
  ctime DateTime CODEC(Delta, LZ4),
  inode UInt64 CODEC(Delta, LZ4),
  nlink UInt64 CODEC(Delta, LZ4),
  INDEX ext_idx ext TYPE set(256) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir_id, name)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
