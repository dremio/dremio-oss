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
package com.dremio.service.sqlrunner;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public class SQLRunnerOptions {
  public static final TypeValidators.BooleanValidator SQLRUNNER_TABS_UI =
      new TypeValidators.BooleanValidator("sqlrunner.tabs_ui", true);

  public static final TypeValidators.BooleanValidator SQLRUNNER_USE_SHOW_CONTEXT =
      new TypeValidators.BooleanValidator("sqlrunner.useshowcontext.enabled", false);
}
