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

package com.dremio.exec.store.parquet;

import java.util.Optional;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Converts the values in the dictionary values. Applicable only for dictionary encoded parquets.
 */
public interface ParquetDictionaryConvertor {

  /**
   * The buffer is expected to have dictionary values. This method does re-arrangement within same
   * buffer.
   *
   * @param dictionary
   * @param colName
   */
  void convertDictionaryBuffer(
      final ArrowBuf dictionary, final int dictionarySize, final Optional<String> colName);

  ParquetDictionaryConvertor DEFAULT =
      (dictionary, dictionarySize, colName) -> {
        // Do nothing by default
      };
}
