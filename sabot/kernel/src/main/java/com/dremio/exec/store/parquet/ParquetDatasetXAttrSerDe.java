/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.parquet;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.ParquetDatasetXAttr;

/**
 * SerDe for parquet specific attributes.
 */
public class ParquetDatasetXAttrSerDe {
  public static final Serializer<ParquetDatasetXAttr> PARQUET_DATASET_XATTR_SERIALIZER = ProtostuffSerializer.of(ParquetDatasetXAttr.getSchema());
  public static final Serializer<ParquetDatasetSplitXAttr> PARQUET_DATASET_SPLIT_XATTR_SERIALIZER = ProtostuffSerializer.of(ParquetDatasetSplitXAttr.getSchema());
}
