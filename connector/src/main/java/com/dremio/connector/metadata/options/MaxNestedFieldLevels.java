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
package com.dremio.connector.metadata.options;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import java.util.stream.Stream;

/** Max allowed nested levels within a complex type column */
public class MaxNestedFieldLevels extends IntMetadataOption
    implements GetDatasetOption, GetMetadataOption, ListPartitionChunkOption {
  public MaxNestedFieldLevels(int numLevels) {
    super(numLevels);
  }

  public static Integer getCount(MetadataOption... options) {
    return Stream.of(options)
        .filter(o -> o instanceof MaxNestedFieldLevels)
        .findFirst()
        .map(o -> ((MaxNestedFieldLevels) o).getValue())
        .orElse(null);
  }
}
