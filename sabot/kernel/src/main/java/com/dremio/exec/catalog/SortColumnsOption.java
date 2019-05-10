/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.catalog;

import java.util.List;
import java.util.stream.Stream;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;

public class SortColumnsOption implements GetMetadataOption, GetDatasetOption, ListPartitionChunkOption {

 private final List<String> sortColumns;

  public SortColumnsOption(List<String> sortColumns) {
    super();
    this.sortColumns = sortColumns;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public static List<String> getSortColumns(MetadataOption... options) {
    return Stream.of(options).filter(o -> o instanceof SortColumnsOption).findFirst().map(o -> ((SortColumnsOption) o).getSortColumns()).orElse(null);
  }
}
