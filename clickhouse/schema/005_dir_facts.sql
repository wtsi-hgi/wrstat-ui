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

CREATE TABLE IF NOT EXISTS wrstat_dir_facts (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir_id UInt32 CODEC(Delta, LZ4),
  parent_id UInt32 CODEC(Delta, LZ4),
  subtree_end UInt32 CODEC(Delta, LZ4),
  updated_at DateTime CODEC(Delta, ZSTD(3)),
  all_count UInt64 CODEC(Delta, LZ4),
  all_size UInt64 CODEC(Delta, LZ4),
  all_atime_min Int64 CODEC(Delta, LZ4),
  all_mtime_max Int64 CODEC(Delta, LZ4),
  all_atime_buckets Array(UInt64) CODEC(LZ4),
  all_mtime_buckets Array(UInt64) CODEC(LZ4),
  all_uids Array(UInt32) CODEC(LZ4),
  all_gids Array(UInt32) CODEC(LZ4),
  all_ft UInt16,
  file_count UInt64 CODEC(Delta, LZ4),
  file_size UInt64 CODEC(Delta, LZ4),
  file_atime_min Int64 CODEC(Delta, LZ4),
  file_mtime_max Int64 CODEC(Delta, LZ4),
  file_atime_buckets Array(UInt64) CODEC(LZ4),
  file_mtime_buckets Array(UInt64) CODEC(LZ4),
  file_uids Array(UInt32) CODEC(LZ4),
  file_gids Array(UInt32) CODEC(LZ4),
  file_ft UInt16,
  gids Array(UInt32) CODEC(LZ4),
  uids Array(UInt32) CODEC(LZ4),
  fts Array(UInt16) CODEC(LZ4),
  ages Array(UInt8) CODEC(LZ4),
  counts Array(UInt64) CODEC(Delta, LZ4),
  sizes Array(UInt64) CODEC(Delta, LZ4),
  atime_mins Array(Int64) CODEC(Delta, LZ4),
  mtime_maxs Array(Int64) CODEC(Delta, LZ4),
  atime_buckets Array(Array(UInt64)) CODEC(LZ4),
  mtime_buckets Array(Array(UInt64)) CODEC(LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3)),
  PROJECTION children_proj (
    SELECT * ORDER BY (mount_path, snapshot_id, parent_id, dir_id)
  )
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir_id)
SETTINGS index_granularity = 8192;
