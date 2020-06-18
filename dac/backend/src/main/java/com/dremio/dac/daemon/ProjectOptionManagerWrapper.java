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
package com.dremio.dac.daemon;

import java.util.Iterator;

import com.dremio.exec.server.options.BaseOptionManager;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.OptionList;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.google.common.collect.Iterators;

/**
 * {@link OptionManagerWrapper ) for {@link ProjectOptionManager }'s
 */
public class ProjectOptionManagerWrapper extends BaseOptionManager implements ProjectOptionManager {
  private final ProjectOptionManager projectOptionManager;
  private final DefaultOptionManager defaultOptionManager;
  private final OptionValidatorListing optionValidatorListing;

  public ProjectOptionManagerWrapper(ProjectOptionManager projectOptionManager,
                                     DefaultOptionManager defaultOptionManager) {
    super(defaultOptionManager.getOptionValidatorListing());
    this.projectOptionManager = projectOptionManager;
    this.defaultOptionManager = defaultOptionManager;
    this.optionValidatorListing = defaultOptionManager.getOptionValidatorListing();
  }

  @Override
  public boolean isValid(String name) {
    return projectOptionManager.isValid(name);
  }

  @Override
  public boolean isSet(String name) {
    return projectOptionManager.isSet(name);
  }

  @Override
  public boolean setOption(OptionValue value) {
    if (!projectOptionManager.setOption(value)) {
      throw new IllegalArgumentException("Could not set option");
    }
    return true;
  }

  @Override
  public boolean deleteOption(String name, OptionValue.OptionType type) {
    if (!projectOptionManager.deleteOption(name, type)) {
      throw new IllegalArgumentException("Could not delete option");
    }
    return true;
  }

  @Override
  public boolean deleteAllOptions(OptionValue.OptionType type) {
    if (!projectOptionManager.deleteAllOptions(type)) {
      throw new IllegalArgumentException("Could not delete options");
    }
    return true;
  }

  @Override
  public OptionValue getOption(String name) {
    OptionValue result = projectOptionManager.getOption(name);
    if (result == null) {
      return defaultOptionManager.getOption(name);
    }
    return result;
  }

  @Override
  protected boolean supportsOptionType(OptionValue.OptionType type) {
    return type == OptionValue.OptionType.SYSTEM;
  }

  @Override
  public OptionList getDefaultOptions() {
    return defaultOptionManager.getDefaultOptions();
  }

  @Override
  public OptionList getNonDefaultOptions() {
    return projectOptionManager.getNonDefaultOptions();
  }

  @Override
  public OptionValidatorListing getOptionValidatorListing() {
    return optionValidatorListing;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterators.concat(projectOptionManager.iterator(), defaultOptionManager.iterator());
  }
}
