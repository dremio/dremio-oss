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
package com.dremio.exec.server.options;

import java.util.Map;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;

/**
 * An OptionManager that eagerly loads all non-default options and caches them in memory.
 * Writes-through to the delegate OptionManager.
 */
public class EagerCachingOptionManager extends InMemoryOptionManager {
  private final OptionManager delegate;

  public EagerCachingOptionManager(OptionManager delegate) {
    super(delegate.getOptionValidatorListing(), listToMap(delegate));
    this.delegate = delegate;
  }

  private static Map<String, OptionValue> listToMap(OptionManager delegate) {
    Map<String, OptionValue> optionMap = CaseInsensitiveMap.newHashMap();

    OptionList optionList = delegate.getNonDefaultOptions();
    if (optionList != null && !optionList.isEmpty()) {
      optionList.forEach(optionValue -> optionMap.put(optionValue.getName(), optionValue));
    }
    return optionMap;
  }

  @Override
  public boolean setOption(OptionValue value) {
    return super.setOption(value) && delegate.setOption(value);
  }

  @Override
  public boolean deleteOption(String name, OptionType type) {
    return super.deleteOption(name, type) && delegate.deleteOption(name, type);
  }

  @Override
  public boolean deleteAllOptions(OptionType type) {
    return super.deleteAllOptions(type) && delegate.deleteAllOptions(type);
  }

  @Override
  public double getOption(DoubleValidator validator) {
    return getOption(validator.getOptionName()).getFloatVal();
  }

  @Override
  public long getOption(LongValidator validator) {
    return getOption(validator.getOptionName()).getNumVal();
  }

  @Override
  public String getOption(StringValidator validator) {
    return getOption(validator.getOptionName()).getStringVal();
  }

  @Override
  public OptionValidatorListing getOptionValidatorListing() {
    return delegate.getOptionValidatorListing();
  }

  @Override
  protected boolean supportsOptionType(OptionType type) {
    return true;
  }
}
