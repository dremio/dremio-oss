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
package com.dremio.test.specs;

import java.util.HashMap;
import java.util.Map;

import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators;

/**
 * Spec for creating an OptionManager
 */
public class OptionResolverSpec implements ISpec<OptionResolver> {
  public static final OptionResolverSpec DEFAULT = new OptionResolverSpec();
  Map<TypeValidators.BooleanValidator, Boolean> booleanOptions = new HashMap<>();
  Map<TypeValidators.DoubleValidator, Double> doubleOptions = new HashMap<>();
  Map<TypeValidators.LongValidator, Long> longOptions = new HashMap<>();

  public OptionResolverSpec addOption(TypeValidators.BooleanValidator bv, boolean v) {
    booleanOptions.put(bv, v);
    return this;
  }

  public OptionResolverSpec addOption(TypeValidators.DoubleValidator bv, double v) {
    doubleOptions.put(bv, v);
    return this;
  }

  public OptionResolverSpec addOption(TypeValidators.LongValidator lv, long v) {
    longOptions.put(lv, v);
    return this;
  }
}
