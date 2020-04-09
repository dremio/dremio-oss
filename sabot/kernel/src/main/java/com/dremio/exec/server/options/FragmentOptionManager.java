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

import java.util.Iterator;
import java.util.Locale;
import java.util.SortedSet;

import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.google.common.collect.TreeMultimap;

/**
 * {@link OptionManager} that holds options within a fragment.
 */
public class FragmentOptionManager extends BaseOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentOptionManager.class);

  private final DefaultOptionManager fallback;
  private final TreeMultimap<String, OptionValue> optionMap;

  public FragmentOptionManager(DefaultOptionManager defaultOptionManager, OptionList nonDefaultOptions) {
    this.fallback = defaultOptionManager;
    this.optionMap = getMapFromOptionList(nonDefaultOptions);
  }

  /**
   * Convert this OptionList to a Map from name to OptionValue
   */
  private static TreeMultimap<String, OptionValue> getMapFromOptionList(final OptionList list) {
    final TreeMultimap<String, OptionValue> optionMap = TreeMultimap.create();
    list.forEach(value -> optionMap.put(value.getName(), value));
    return optionMap;
  }

  protected boolean supportsOptionType(OptionType type) {
    throw new UnsupportedOperationException("FragmentOptionManager does not support the given option value.");
  }

  @Override
  public void setOption(OptionValue value) {
    throw new UnsupportedOperationException("FragmentOptionManager does not support options mutation.");
  }

  @Override
  public void deleteOption(String name, OptionType type) {
    throw new UnsupportedOperationException("FragmentOptionManager does not support options deletion.");
  }

  @Override
  public void deleteAllOptions(OptionType type) {
    throw new UnsupportedOperationException("FragmentOptionManager does not support options deletions for all.");
  }

  @Override
  public OptionValue getOption(final String name) {
    final SortedSet<OptionValue> values = optionMap.get(name.toLowerCase(Locale.ROOT));
    if (!values.isEmpty()) {
      return values.last();
    }
    return fallback.getOption(name);
  }

  @Override
  public OptionList getOptionList() {
    final OptionList result = new OptionList();
    final TreeMultimap<String, OptionValue> buildMap = TreeMultimap.create(optionMap);
    fallback.iterator().forEachRemaining(value -> buildMap.put(value.getName(), value));
    result.addAll(buildMap.values());
    return result;
  }

  @Override
  public OptionValidator getValidator(String name) {
    return fallback.getValidator(name);
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return getOptionList().iterator();
  }

}
