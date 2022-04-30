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
package com.dremio.exec.expr.fn;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.options.OptionManager;

/**
 * Used to provide v2 decimal implementation.
 * This would use the correct type precedence for decimal vs double.
 * Also use the new functions that return decimal instead of double.
 */
public class DecimalFunctionImplementationRegistry extends FunctionImplementationRegistry {
  public DecimalFunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan, OptionManager optionManager) {
    super(config, classpathScan, optionManager);
  }

  protected void initializePrimaryRegistries() {
    // this is the registry used, when decimal v2 is turned off.
    isDecimalV2Enabled = true;
    // order is important, first lookup java functions and then gandiva functions
    // if gandiva is preferred code generator, the function would be replaced later.
    primaryFunctionRegistries.add(functionRegistry);
    primaryFunctionRegistries.add(new GandivaFunctionRegistry(isDecimalV2Enabled, getOptionManager()));
  }
}
