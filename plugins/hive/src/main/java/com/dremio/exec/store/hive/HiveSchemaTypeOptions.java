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
package com.dremio.exec.store.hive;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;

public class HiveSchemaTypeOptions {

  private final boolean parquetComplexTypesEnabled;
  private final boolean mapTypeEnabled;
  private final boolean nativeComplexTypesEnabled;
  private final boolean deltaColumnMappingEnabled;

  public HiveSchemaTypeOptions(boolean parquetComplexTypesEnabled, boolean mapTypeEnabled,
                               boolean nativeComplexTypesEnabled, boolean deltaColumnMappingEnabled) {
    this.parquetComplexTypesEnabled = parquetComplexTypesEnabled;
    this.mapTypeEnabled = mapTypeEnabled;
    this.nativeComplexTypesEnabled = nativeComplexTypesEnabled;
    this.deltaColumnMappingEnabled = deltaColumnMappingEnabled;
  }

  public HiveSchemaTypeOptions(final OptionManager optionManager) {
    this(optionManager.getOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED),
         optionManager.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE),
         optionManager.getOption(ExecConstants.ENABLE_COMPLEX_HIVE_DATA_TYPE),
         optionManager.getOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING));
  }

  public boolean isParquetComplexTypesEnabled() {
    return parquetComplexTypesEnabled;
  }

  public boolean isMapTypeEnabled() {
    return mapTypeEnabled;
  }

  public boolean isNativeComplexTypesEnabled() {
    return nativeComplexTypesEnabled;
  }

  public boolean isDeltaColumnMappingEnabled() {
    return deltaColumnMappingEnabled;
  }
}
