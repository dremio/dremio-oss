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

/** This is the schema for sys."cache_manager_datasets" */
public class CacheManagerDatasetInfo {
  public final String hostname;
  public final String dataset_name;
  public final String storage_plugin_name;
  public final long approx_file_count;
  public final Timestamp percent_data_25;
  public final Timestamp percent_data_50;
  public final Timestamp percent_data_75;
  public final Timestamp percent_data_100;

  public CacheManagerDatasetInfo(
      String hostname,
      String datasetName,
      String storagePluginName,
      long approxFileCount,
      Timestamp percent_data_25,
      Timestamp percent_data_50,
      Timestamp percent_data_75,
      Timestamp percent_data_100) {
    this.hostname = hostname;
    this.dataset_name = datasetName;
    this.storage_plugin_name = storagePluginName;
    this.approx_file_count = approxFileCount;
    this.percent_data_25 = percent_data_25;
    this.percent_data_50 = percent_data_50;
    this.percent_data_75 = percent_data_75;
    this.percent_data_100 = percent_data_100;
  }
}
