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

package com.dremio.exec.physical.base;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@JsonDeserialize(builder = ImmutableCombineSmallFileOptions.Builder.class)
@Value.Immutable
public interface CombineSmallFileOptions {

  // the threshold to be defined as small files
  @Nullable
  Long getSmallFileSize();

  /**
   * the target file size after combination. Usually, it is the same as PARQUET_BLOCK_SIZE. We
   * define it here to let tests define customized target file size
   */
  @Nullable
  Long getTargetFileSize();

  static ImmutableCombineSmallFileOptions.Builder builder() {
    return new ImmutableCombineSmallFileOptions.Builder();
  }
}
