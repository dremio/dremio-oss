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
package com.dremio.exec.store.hive.exec;

import java.util.Map;
import java.util.Optional;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.connector.metadata.AttributeValue;

/**
 * Holds hive table options and helper methods
 */
public final class HiveDatasetOptions {

  public static final String HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH = "enable_varchar_truncation";

  private static final CaseInsensitiveMap<AttributeValue> dafaultValueMap = CaseInsensitiveMap.newHashMap();

  static {
    dafaultValueMap.put(HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, AttributeValue.of(false));
  }

  public static Optional<AttributeValue> getDefaultValue(String optionName) {
    return Optional.ofNullable(dafaultValueMap.get(optionName));
  }

  public static AttributeValue getDefaultValueOfKnownOption(String optionName) {
    return dafaultValueMap.get(optionName);
  }

  /**
   * returns value of HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH option if present in map else returns default value
   * @param datasetOptionMap
   * @return
   */
  public static boolean enforceVarcharWidth(Map<String, AttributeValue> datasetOptionMap) {
    AttributeValue enforceVarcharWidth = datasetOptionMap.getOrDefault(HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH,
        getDefaultValueOfKnownOption(HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH));
    return HiveReaderProtoUtil.getBoolean(enforceVarcharWidth);
  }

  private HiveDatasetOptions() {
  }
}
