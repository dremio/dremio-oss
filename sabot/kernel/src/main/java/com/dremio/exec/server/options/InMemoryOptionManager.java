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
import java.util.Map;

import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;

/**
 * {@link OptionManager} that hold options in memory rather than in a persistent store. Option stored in
 * {@link SessionOptionManagerImpl}, {@link QueryOptionManager}, and {@link FragmentOptionManager} are held in memory
 * (see {@link #options}) whereas {@link SystemOptionManager} stores options in a persistent store.
 */
public abstract class InMemoryOptionManager extends BaseOptionManager {

  protected final Map<String, OptionValue> options;

  protected InMemoryOptionManager(final OptionValidatorListing optionValidatorListing, final Map<String, OptionValue> options) {
    super(optionValidatorListing);
    this.options = options;
  }

  @Override
  public boolean setOption(OptionValue value) {
    if (supportsOptionType(value.getType())) {
      options.put(value.getName(), value);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean deleteOption(String name, OptionType type) {
    if (supportsOptionType(type)) {
      options.remove(name);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean deleteAllOptions(OptionType type) {
    if (supportsOptionType(type)) {
      options.clear();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public OptionValue getOption(String name) {
    return options.get(name);
  }

  @Override
  public OptionList getNonDefaultOptions() {
    OptionList buildList = new OptionList();
    buildList.addAll(options.values());
    return buildList;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return options.values().iterator();
  }

}
