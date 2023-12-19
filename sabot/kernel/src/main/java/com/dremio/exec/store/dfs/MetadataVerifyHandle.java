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
package com.dremio.exec.store.dfs;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;

/**
 * An optional interface for a dataset handle to perform ad-hoc metadata verification
 * base on {@link MetadataVerifyRequest} and return a {@link DatasetMetadataVerifyResult}
 */
public interface MetadataVerifyHandle {
  @Nonnull
  Optional<DatasetMetadataVerifyResult> verifyMetadata(MetadataVerifyRequest metadataVerifyRequest);
}
