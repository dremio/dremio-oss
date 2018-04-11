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
package com.dremio.exec.store.dfs;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.service.namespace.file.proto.FileSystemCachedEntity;

/**
 * SerDe for filesystem datasets
 */
public class FileSystemXAttrSerDe {
  public static final Serializer<FileSystemCachedEntity> FILE_SYSTEM_CACHED_ENTITY_SERIALIZER = ProtostuffSerializer.of(FileSystemCachedEntity.getSchema());
}
