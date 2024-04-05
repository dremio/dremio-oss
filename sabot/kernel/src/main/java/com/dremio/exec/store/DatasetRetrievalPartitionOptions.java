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
package com.dremio.exec.store;

import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.options.RefreshTableFilterOption;
import java.util.List;
import java.util.Map;

public class DatasetRetrievalPartitionOptions extends DatasetRetrievalOptions {

  private final Map<String, String> partition;

  DatasetRetrievalPartitionOptions(
      DatasetRetrievalOptions.Builder builder, Map<String, String> partition) {
    super(builder);
    this.partition = partition;
  }

  public Map<String, String> getPartition() {
    return partition;
  }

  @Override
  protected void addCustomOptions(List<ListPartitionChunkOption> options) {
    options.add(new RefreshTableFilterOption(partition));
  }
}
