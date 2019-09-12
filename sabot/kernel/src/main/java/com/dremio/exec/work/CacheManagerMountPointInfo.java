/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.work;

/**
 * This is the schema for sys."cache_manager_mount_points"
 */
public class CacheManagerMountPointInfo {
  public final String hostname;
  public final String mount_point_path;
  public final long mount_point_id;
  public final long sub_dir_count;
  public final long approx_file_count;
  public final long max_space;
  public final long used_space;
  public final long avg_read_time_nanos;
  public final long avg_write_time_nanos;

  public CacheManagerMountPointInfo(String hostname, String mountPointPath, long mountPointId, long subDirCount, long approxFileCount,
                                    long maxSpace, long usedSpace, long avgReadTimeNanos, long avgWriteTimeNanos) {
    this.hostname = hostname;
    this.mount_point_path = mountPointPath;
    this.mount_point_id = mountPointId;
    this.sub_dir_count = subDirCount;
    this.approx_file_count = approxFileCount;
    this.max_space = maxSpace;
    this.used_space = usedSpace;
    this.avg_read_time_nanos = avgReadTimeNanos;
    this.avg_write_time_nanos = avgWriteTimeNanos;
  }
}
