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
package com.dremio.dac.explore.bi;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/**
 * Final class containing constants for BI tools message body generators.
 */
@Options
public final class BIToolsConstants {
  private BIToolsConstants() {}

  /**
   * Option to override the hostname used in the exported messages. Empty to indicate there is no override.
   */
  public static final TypeValidators.StringValidator EXPORT_HOSTNAME =
    new TypeValidators.StringValidator("export.bi.hostname", "");
}
