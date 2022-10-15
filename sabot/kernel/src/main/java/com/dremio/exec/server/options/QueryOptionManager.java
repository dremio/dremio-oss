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

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue.OptionType;

/**
 * {@link OptionManager} that holds options within {@link com.dremio.exec.ops.QueryContext}.
 */
public class QueryOptionManager extends InMemoryOptionManager {

  public QueryOptionManager(final OptionValidatorListing optionValidatorListing) {
    super(optionValidatorListing, CaseInsensitiveMap.newHashMap());
  }

  @Override
  protected boolean supportsOptionType(OptionType type) {
    return type == OptionType.QUERY;
  }
}
