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
package com.dremio.test.shams;

import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Map backed {@link OptionResolver}. */
public class ShamOptionResolver implements OptionResolver {
  public static final OptionResolver DEFAULT_VALUES =
      new ShamOptionResolver(
          ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

  private final Map<BooleanValidator, Boolean> booleanValidators;
  private final Map<DoubleValidator, Double> doubleValidators;
  private final Map<LongValidator, Long> longValidators;
  private final Map<StringValidator, String> stringValidators;

  public ShamOptionResolver(
      Map<BooleanValidator, Boolean> booleanValidators,
      Map<DoubleValidator, Double> doubleValidators,
      Map<LongValidator, Long> longValidators,
      Map<StringValidator, String> stringValidators) {
    this.booleanValidators = booleanValidators;
    this.doubleValidators = doubleValidators;
    this.longValidators = longValidators;
    this.stringValidators = stringValidators;
  }

  @Override
  public boolean getOption(BooleanValidator validator) {
    return booleanValidators.getOrDefault(validator, validator.getDefault().getBoolVal());
  }

  @Override
  public double getOption(DoubleValidator validator) {
    return doubleValidators.getOrDefault(validator, validator.getDefault().getFloatVal());
  }

  @Override
  public long getOption(LongValidator validator) {
    return longValidators.getOrDefault(validator, validator.getDefault().getNumVal());
  }

  @Override
  public String getOption(StringValidator validator) {
    return stringValidators.getOrDefault(validator, validator.getDefault().getStringVal());
  }
}
