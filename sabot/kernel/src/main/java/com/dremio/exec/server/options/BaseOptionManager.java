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

import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;

public abstract class BaseOptionManager implements OptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseOptionManager.class);
  private final OptionValidatorListing optionValidatorListing;

  protected BaseOptionManager(OptionValidatorListing optionValidatorListing) {
    this.optionValidatorListing = optionValidatorListing;
  }

  /**
   * Gets the current option value given a validator.
   *
   * @param validator the validator
   * @return option value
   * @throws IllegalArgumentException - if the validator is not found
   */
  private OptionValue getOptionSafe(OptionValidator validator)  {
    return getOption(validator.getOptionName());
  }

  /**
   * Check to see if implementations of this manager support the given option type.
   *
   * @param type option type
   * @return true iff the type is supported
   */
  abstract protected boolean supportsOptionType(OptionValue.OptionType type);

  @Override
  public boolean getOption(BooleanValidator validator) {
    return getOptionSafe(validator).getBoolVal();
  }

  @Override
  public double getOption(DoubleValidator validator) {
    return getOptionSafe(validator).getFloatVal();
  }

  @Override
  public long getOption(LongValidator validator) {
    return getOptionSafe(validator).getNumVal();
  }

  @Override
  public String getOption(StringValidator validator) {
    return getOptionSafe(validator).getStringVal();
  }

  @Override
  public OptionList getDefaultOptions() {
    return new OptionList();
  }

  @Override
  public OptionList getNonDefaultOptions() {
    return new OptionList();
  }

  @Override
  public OptionValidatorListing getOptionValidatorListing() {
    return optionValidatorListing;
  }
}
