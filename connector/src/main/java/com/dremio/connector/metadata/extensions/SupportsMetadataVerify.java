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
package com.dremio.connector.metadata.extensions;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * This is an optional interface. When implemented by a plugin, this is used to perform ad-hoc
 * metadata verification base on {@link MetadataVerifyRequest} and return a {@link
 * DatasetMetadataVerifyResult}
 */
public interface SupportsMetadataVerify {
  @Nonnull
  Optional<DatasetMetadataVerifyResult> verifyMetadata(
      DatasetHandle datasetHandle, MetadataVerifyRequest metadataVerifyRequest);
}
