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

CREATE TABLE IF NOT EXISTS wrstat_dirs (
  mount_path LowCardinality(String) CODEC(LZ4),
  snapshot_id UUID,
  dir_id UInt32 CODEC(Delta, LZ4),
  parent_id UInt32 CODEC(Delta, LZ4),
  subtree_end UInt32 CODEC(Delta, LZ4),
  depth UInt16 CODEC(Delta, LZ4),
  name String CODEC(ZSTD(3)),
  full_path String CODEC(ZSTD(3)),
  child_dir_count UInt32 CODEC(Delta, LZ4),
  child_file_count UInt32 CODEC(Delta, LZ4),
  path_hash UInt64 CODEC(LZ4),
  INDEX full_path_ngram full_path TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4,
  INDEX full_path_tokens full_path TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 4,
  PROJECTION children_proj (
    SELECT * ORDER BY (mount_path, snapshot_id, parent_id, dir_id)
  ),
  PROJECTION path_hash_proj (
    SELECT * ORDER BY (mount_path, snapshot_id, path_hash)
  )
) ENGINE = MergeTree
PARTITION BY (mount_path, snapshot_id)
ORDER BY (mount_path, snapshot_id, dir_id)
SETTINGS index_granularity = 8192;
