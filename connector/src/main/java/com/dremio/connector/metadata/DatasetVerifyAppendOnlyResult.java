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

import com.dremio.connector.metadata.extensions.SupportsMetadataVerify;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.connector.metadata.options.VerifyAppendOnlyRequest;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;

/**
 * Metadata verify result returned by {@link SupportsMetadataVerify#verifyMetadata(DatasetHandle,
 * MetadataVerifyRequest)} for metadata verify request of {@link VerifyAppendOnlyRequest}
 */
@Value.Immutable
public interface DatasetVerifyAppendOnlyResult extends DatasetMetadataVerifyResult {
  enum ResultCode {
    APPEND_ONLY,
    NOT_APPEND_ONLY,
    NOT_ANCESTOR,
    INVALID_BEGIN_SNAPSHOT,
    INVALID_END_SNAPSHOT
  }

  ResultCode getResultCode();

  List<Pair<String, String>> getSnapshotRanges();

  static ImmutableDatasetVerifyAppendOnlyResult.Builder builder() {
    return ImmutableDatasetVerifyAppendOnlyResult.builder();
  }
}
