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
package com.dremio.options;

import java.util.HashMap;
import java.util.Map;

import com.dremio.options.OptionValue.OptionType;

/**
 * Manager for Dremio {@link OptionValue options}. Implementations must be case-insensitive to the name of an option.
 */
public interface OptionManager extends Iterable<OptionValue>, OptionResolver {

  /**
   * Sets an option value.
   *
   * @param value option value
   * @throws com.dremio.common.exceptions.UserException message to describe error with value
   */
  boolean setOption(OptionValue value);

  /**
   * Deletes the option if the option name is valid. Does nothing if the option is not set.
   *
   * @param name option name
   * @param type option type
   * @throws com.dremio.common.exceptions.UserException message to describe error with value
   */
  boolean deleteOption(String name, OptionType type);

  /**
   * Deletes all options of a OptionType. Does nothing if no options are set.
   *
   * @param type option type
   * @throws com.dremio.common.exceptions.UserException message to describe error with value
   */
  boolean deleteAllOptions(OptionType type);

  /**
   * Gets the option value for the given option name.
   *
   * @param name option name
   * @return the option value, null if the option does not exist
   */
  OptionValue getOption(String name);

  /**
   * Gets the list of default options if this Option Manager contains it
   *
   * @return the list of default options. Returns an empty list if there are no default options.
   */
  OptionList getDefaultOptions();

  /**
   * Gets the list of non-default options if this Option Manager contains it
   *
   * @return the list of non-default options. Returns an empty list if there are no non-default options
   */
  OptionList getNonDefaultOptions();


  /**
   * Get the OptionValidatorProvider in this Option Manager.
   * @return the OptionValidatorProvider
   */
  OptionValidatorListing getOptionValidatorListing();

  default void addOptionChangeListener(OptionChangeListener optionChangeListener) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Adding option change listener not supported");
  }

  default Map<String, String> optionsAsMap(String... keys) {
    Map<String, String> props = new HashMap<>();
    for (String key : keys) {
      props.put(key, String.valueOf(getOption(key).getValue()));
    }
    return props;
  }
}
