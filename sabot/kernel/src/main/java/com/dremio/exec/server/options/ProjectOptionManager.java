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

import com.dremio.options.OptionManager;

/**
 * Interface for options set at the project level affect the entire project.
 */
public interface ProjectOptionManager extends OptionManager {

  /**
   * Check if name is a valid option key.
   *
   * @param name option name
   * @return boolean, true if validators contain name as a key.
   */
  boolean isValid(String name);

  /**
   * Check if the value of the given option name exists.
   *
   * @param name option name
   * @return boolean, true if the option value is not null
   */
  boolean isSet(String name);
}
