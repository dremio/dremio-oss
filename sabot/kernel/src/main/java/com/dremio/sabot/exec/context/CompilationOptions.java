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
package com.dremio.sabot.exec.context;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.CachingOptionResolver;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionResolver;

/** Holds options associated with code generation. */
public class CompilationOptions {

  private final OptionResolver optionResolver;

  public CompilationOptions(OptionManager options) {
    this.optionResolver = new CachingOptionResolver(options);
  }

  public boolean enableOrOptimization() {
    return optionResolver.getOption(ExecConstants.FAST_OR_ENABLE);
  }

  public int getOrOptimizationThreshold() {
    return (int) optionResolver.getOption(ExecConstants.FAST_OR_MIN_THRESHOLD);
  }

  public int getVarcharOrOptimizationThreshold() {
    return (int) optionResolver.getOption(ExecConstants.FAST_OR_MIN_VARCHAR_THRESHOLD);
  }

  public int getOrOptimizationThresholdForGandiva() {
    return (int) optionResolver.getOption(ExecConstants.FAST_OR_MIN_THRESHOLD_GANDIVA);
  }

  public int getVarcharOrOptimizationThresholdForGandiva() {
    return (int) optionResolver.getOption(ExecConstants.FAST_OR_MIN_VARCHAR_THRESHOLD_GANDIVA);
  }

  public int getNewMethodThreshold() {
    return (int) optionResolver.getOption(ExecConstants.CODE_GEN_NESTED_METHOD_THRESHOLD);
  }

  public int getConstantArrayThreshold() {
    return (int) optionResolver.getOption(ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD);
  }

  public long getFunctionExpressionCountThreshold() {
    return optionResolver.getOption(ExecConstants.CODE_GEN_FUNCTION_EXPRESSION_COUNT_THRESHOLD);
  }

  public OptionResolver getOptionResolver() {
    return optionResolver;
  }
}
