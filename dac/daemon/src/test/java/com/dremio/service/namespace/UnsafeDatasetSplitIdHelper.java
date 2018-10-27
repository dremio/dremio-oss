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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class to generate unsafe dataset split ids
 */
@VisibleForTesting
public final class UnsafeDatasetSplitIdHelper {

  private UnsafeDatasetSplitIdHelper() {
  }


  public static DatasetSplitId of(DatasetConfig config, String key) {
    return DatasetSplitId.ofUnsafe(config.getId(), config.getReadDefinition().getSplitVersion(), key);
  }

}
