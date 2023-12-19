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
package com.dremio.options.impl;

import java.util.Iterator;

import com.dremio.options.OptionList;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;

/**
 * Option manager for static defaults.
 */
public class DefaultOptionManager extends BaseOptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultOptionManager.class);

  private final OptionValidatorListing optionValidatorListing;

  public DefaultOptionManager(OptionValidatorListing optionValidatorListing) {
      super(optionValidatorListing);
      this.optionValidatorListing = optionValidatorListing;
  }

  @Override
  public boolean setOption(OptionValue value) {
    throw new UnsupportedOperationException("Cannot set option in DefaultOptionManager");
  }

  @Override
  public boolean deleteOption(String name, OptionValue.OptionType type) {
    throw new UnsupportedOperationException("Cannot delete option in DefaultOptionManager");
  }

  @Override
  public boolean deleteAllOptions(OptionValue.OptionType type) {
    throw new UnsupportedOperationException("Cannot delete all options in DefaultOptionManager");
  }

  @Override
  public OptionValue getOption(String name) {
    final OptionValidator optionValidator = optionValidatorListing.getValidator(name);
    return optionValidator.getDefault();
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return getDefaultOptions().iterator();
  }

  @Override
  public OptionList getDefaultOptions() {
    OptionList result = new OptionList();
    optionValidatorListing.iterator().forEachRemaining(
      validator -> result.add(validator.getDefault())
    );
    return result;
  }

  @Override
  protected boolean supportsOptionType(OptionValue.OptionType type) {
    return false;
  }
}
