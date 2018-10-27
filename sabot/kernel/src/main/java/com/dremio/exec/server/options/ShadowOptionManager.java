/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import com.dremio.options.OptionValue;

/**
 * {@link OptionManager} that does not validate values for options.
 * Instead the options shadow validated options. Intended to be used when
 * the option takes a value that has been validated by a Validator
 *
 * This creates options that cannot be changed directly from the Admin UI,
 * but the options take values from other validated options
 */
public class ShadowOptionManager extends QueryOptionManager {
  public ShadowOptionManager(OptionManager options) {
    super(options);
  }

  // NOTE: This does not check the validity of the value
  // Hence, the value should be set using the value from another validated option
  @Override
  public void setOption(OptionValue value) {
    setLocalOption(value);
  }
}
