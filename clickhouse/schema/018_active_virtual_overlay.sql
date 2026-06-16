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

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_dirs (
  active_set_id String CODEC(ZSTD(3)),
  virtual_id UInt32 CODEC(Delta, LZ4),
  parent_id UInt32 CODEC(Delta, LZ4),
  name String CODEC(ZSTD(3)),
  full_path String CODEC(ZSTD(3)),
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  mount_root_dir_id UInt32 CODEC(Delta, LZ4),
  is_mount_root_box UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3)),
  INDEX full_path_tokens full_path TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 4,
  PROJECTION parent_proj (
    SELECT * ORDER BY (active_set_id, parent_id, virtual_id)
  )
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, virtual_id);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_summaries (
  active_set_id String CODEC(ZSTD(3)),
  virtual_id UInt32 CODEC(Delta, LZ4),
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  mount_root_dir_id UInt32 CODEC(Delta, LZ4),
  is_mount_root_box UInt8,
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
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, virtual_id);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_filter_all (
  active_set_id String CODEC(ZSTD(3)),
  virtual_id UInt32 CODEC(Delta, LZ4),
  age UInt8,
  gid UInt32,
  uid UInt32,
  ft UInt16,
  count UInt64 CODEC(Delta, LZ4),
  size UInt64 CODEC(Delta, LZ4),
  atime_min Int64 CODEC(Delta, LZ4),
  mtime_max Int64 CODEC(Delta, LZ4),
  atime_buckets Array(UInt64) CODEC(LZ4),
  mtime_buckets Array(UInt64) CODEC(LZ4),
  filter_child_count UInt64 CODEC(Delta, LZ4),
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, virtual_id, age, gid, uid, ft);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_children (
  active_set_id String CODEC(ZSTD(3)),
  parent_virtual_id UInt32 CODEC(Delta, LZ4),
  child_virtual_id UInt32 CODEC(Delta, LZ4),
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  mount_root_dir_id UInt32 CODEC(Delta, LZ4),
  is_mount_root_box UInt8,
  child_count UInt64 CODEC(Delta, LZ4),
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY (active_set_id, parent_virtual_id, child_virtual_id);

CREATE TABLE IF NOT EXISTS wrstat_active_virtual_sets (
  active_set_id String CODEC(ZSTD(3)),
  schema3_version UInt32 CODEC(Delta, ZSTD(3)),
  mounts_sha256 String CODEC(ZSTD(3)),
  active_mount_count UInt64 CODEC(Delta, ZSTD(3)),
  summary_rows UInt64 CODEC(Delta, ZSTD(3)),
  filter_rows UInt64 CODEC(Delta, ZSTD(3)),
  child_rows UInt64 CODEC(Delta, ZSTD(3)),
  manifest_sha256 String CODEC(ZSTD(3)),
  ready UInt8,
  refreshed_at DateTime64(3) CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree
PARTITION BY active_set_id
ORDER BY active_set_id;
