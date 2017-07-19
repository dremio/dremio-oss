/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * {@link OptionManager} that holds options within a fragment.
 */
public class FragmentOptionManager extends InMemoryOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentOptionManager.class);

  public FragmentOptionManager(OptionManager systemOptions, OptionList options) {
    super(systemOptions, getMapFromOptionList(options));
  }

  private static Map<String, OptionValue> getMapFromOptionList(final OptionList options) {
    final Map<String, OptionValue> tmp = Maps.newHashMap();
    for (final OptionValue value : options) {
      tmp.put(value.name, value);
    }
    return CaseInsensitiveMap.newImmutableMap(tmp);
  }

  @Override
  boolean supportsOptionType(OptionType type) {
    throw new UnsupportedOperationException("FragmentOptionManager does not support the given option value.");
  }


}
