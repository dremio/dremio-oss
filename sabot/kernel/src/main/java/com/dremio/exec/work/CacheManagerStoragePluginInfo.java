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
 * This is the schema for sys."cache_manager_storage_plugins"
 */
public class CacheManagerStoragePluginInfo {
  public final String hostname;
  public final String storage_plugin_name;
  public final long storage_plugin_id;
  public final long sub_dir_count;
  public final long approx_file_count;
  public final long approx_size_bytes;

  public CacheManagerStoragePluginInfo(String hostname, String storagePluginName, long storagePluginId, long subDirCount,
                                       long approxFileCount, long approxSizeBytes) {
    this.hostname = hostname;
    this.storage_plugin_name = storagePluginName;
    this.storage_plugin_id = storagePluginId;
    this.sub_dir_count = subDirCount;
    this.approx_file_count = approxFileCount;
    this.approx_size_bytes = approxSizeBytes;
  }
}
