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

import java.sql.Timestamp;

/**
 * This is the schema for sys."cache_manager_files"
 */
public class CacheManagerFilesInfo {
  public final String hostname;
  public final String plugin;
  public final String dataset;
  public final String path;
  public final String version;
  public final long offset;
  public final Timestamp atime;

  public CacheManagerFilesInfo(String hostname, String plugin, String dataset, String path, String version, long offset, Timestamp atime) {
    this.hostname = hostname;
    this.plugin = plugin;
    this.dataset = dataset;
    this.path = path;
    this.version = version;
    this.offset = offset;
    this.atime = atime;
  }
}
