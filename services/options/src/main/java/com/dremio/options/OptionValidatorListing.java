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

import java.util.Collection;
import java.util.Iterator;

/**
 * Provides OptionValidators
 */
public interface OptionValidatorListing {
  /**
   * Get the option validator for a given name
   *
   * @param name the option name
   * @return the validator
   * @throws com.dremio.common.exceptions.UserException if option doesn't exist
   */
  OptionValidator getValidator(String name);

  /**
   * Check if name is a valid option key.
   *
   * @param name option name
   * @return boolean, true if validators contain name as a key.
   */
  boolean isValid(String name);

  /**
   * Get all option validators
   *
   * @return all validators
   */
  Collection<OptionValidator> getValidatorList();

  /**
   * Get all option validators
   *
   * @return iterator of all option validators
   */
  Iterator<? extends OptionValidator> iterator();
}
