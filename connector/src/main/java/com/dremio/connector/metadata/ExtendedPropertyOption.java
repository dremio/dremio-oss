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
package com.dremio.connector.metadata;

import java.util.Optional;
import java.util.stream.Stream;


/**
 * Option passed to {@link SourceMetadata#getDatasetMetadata} and {@link SourceMetadata#listPartitionChunks}
 */
public class ExtendedPropertyOption implements GetMetadataOption, ListPartitionChunkOption {

  private final BytesOutput extendedProperty;

  public ExtendedPropertyOption(BytesOutput extendedProperty) {
    this.extendedProperty = extendedProperty;
  }

  public BytesOutput getExtendedProperty() {
    return extendedProperty;
  }

  public static Optional<BytesOutput> getExtendedPropertyFromMetadataOption(MetadataOption... options) {
    return Stream.of(options).filter(o -> o instanceof ExtendedPropertyOption).findFirst().map(o -> ((ExtendedPropertyOption) o).getExtendedProperty());
  }

  public static Optional<BytesOutput> getExtendedPropertyFromListPartitionChunkOption(ListPartitionChunkOption... options) {
    return Stream.of(options).filter(o -> o instanceof ExtendedPropertyOption).findFirst().map(o -> ((ExtendedPropertyOption) o).getExtendedProperty());
  }

}
