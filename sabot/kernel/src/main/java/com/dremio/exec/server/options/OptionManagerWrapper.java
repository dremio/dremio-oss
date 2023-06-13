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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link OptionManager} that handles fallback logic for other {@link OptionManager}'s.
 * OptionManagers should not handle lifecycle of specific {@link OptionManager}'s.
 */
public final class OptionManagerWrapper extends BaseOptionManager {
  private final List<OptionManager> optionManagers;
  private final OptionValidatorListing optionValidatorListing;

  OptionManagerWrapper(OptionValidatorListing optionValidatorListing, List<OptionManager> optionManagers) {
    super(optionValidatorListing);
    this.optionManagers = optionManagers;
    this.optionValidatorListing = optionValidatorListing;
  }

  @Override
  public OptionValidatorListing getOptionValidatorListing() {
    return optionValidatorListing;
  }

  @VisibleForTesting
  public List<OptionManager> getOptionManagers() {
    return optionManagers;
  }

  public static class Builder {
    private OptionValidatorListing optionValidatorListing = null;
    private final LinkedList<OptionManager> optionManagerList = new LinkedList<>();

    public static OptionManagerWrapper.Builder newBuilder() {
      return new OptionManagerWrapper.Builder();
    }

    /**
     * Explicitly set a {@link OptionValidatorListing}.
     */
    public Builder withOptionValidatorProvider(OptionValidatorListing optionValidatorListing) {
      this.optionValidatorListing = optionValidatorListing;
      return this;
    }

    /**
     * Add an {@link OptionManager} to add it to the fallback structure. {@link OptionManager}'s
     * should be added in reverse order (later managers have higher priority). Another {@link OptionManagerWrapper}
     * can be added, in which case all {@link OptionManager}'s are added to the fallback structure.
     * {@link OptionValidatorListing} is inferred from provided {@link OptionManager}.
     */
    public Builder withOptionManager(OptionManager optionManager) {
      if (optionManager instanceof OptionManagerWrapper) {
        final OptionManagerWrapper optionManagerWrapper = (OptionManagerWrapper) optionManager;
        final List<OptionManager> optionManagerList = optionManagerWrapper.getOptionManagers();

        this.optionManagerList.addAll(0, optionManagerList);
        this.optionValidatorListing = optionManagerWrapper.getOptionValidatorListing();
      } else {
        if (optionValidatorListing == null) {
          optionValidatorListing = optionManager.getOptionValidatorListing();
        }
        optionManagerList.addFirst(optionManager);
      }
      return this;
    }

    public OptionManagerWrapper build() {
      Preconditions.checkNotNull(optionValidatorListing, "Builder was not provided with an OptionValidatorProvider.");
      Preconditions.checkArgument(!optionManagerList.isEmpty(), "Builder was not provided with any optionManagers.");
      return new OptionManagerWrapper(optionValidatorListing, optionManagerList);
    }
  }

  protected final <T> T handleFallback(Function<OptionManager, T> handler) {
    for (OptionManager optionManager : optionManagers) {
      if (optionManager != null) {
        T result = handler.apply(optionManager);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }

  protected final boolean handleFallbackBoolean(Function<OptionManager, Boolean> handler) {
    for (OptionManager optionManager : optionManagers) {
      if (optionManager != null) {
        Boolean result = handler.apply(optionManager);
        if (result) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean setOption(OptionValue value) {
    getValidator(value.getName()); // ensure option exists
    return handleFallbackBoolean(optionManager -> optionManager.setOption(value));
  }

  @Override
  public boolean deleteOption(String name, OptionValue.OptionType type) {
    getValidator(name); // ensure option exists
    return handleFallbackBoolean(optionManager -> optionManager.deleteOption(name, type));
  }

  @Override
  public boolean deleteAllOptions(OptionValue.OptionType type) {
    return handleFallbackBoolean(optionManager -> optionManager.deleteAllOptions(type));
  }

  @Override
  public OptionValue getOption(String name) {
    return handleFallback(optionManager -> optionManager.getOption(name));
  }

  @Override
  public OptionList getDefaultOptions() {
    final OptionList optionList = new OptionList();
    for (OptionManager optionManager : optionManagers) {
      OptionList defaultOptions = optionManager.getDefaultOptions();
      optionList.merge(defaultOptions);
    }
    return optionList;
  }

  @Override
  public OptionList getNonDefaultOptions() {
    final OptionList optionList = new OptionList();
    for (OptionManager optionManager : optionManagers) {
      OptionList nonDefaultOptions = optionManager.getNonDefaultOptions();
      optionList.merge(nonDefaultOptions);
    }
    return optionList;
  }

  public OptionValidator getValidator(String name) {
    return optionValidatorListing.getValidator(name);
  }

  @Override
  public Iterator<OptionValue> iterator() {
    final OptionList resultList = new OptionList();
    final Map<String, OptionValue> optionsMap = CaseInsensitiveMap.newHashMap();
    final OptionList defaultOptions = getDefaultOptions();

    // Add default options to optionMap
    defaultOptions.forEach(optionValue -> optionsMap.put(optionValue.getName(), optionValue));

    final List<OptionManager> reversedOptionManagers = Lists.reverse(optionManagers);
    for (OptionManager optionManager : reversedOptionManagers) {
      OptionList optionList = optionManager.getNonDefaultOptions();
      for (OptionValue optionValue : optionList) {
        // If option types match overwrite existing (and default) options
        // Otherwise just add it to the resultList
        if (optionValue.getType() == optionsMap.get(optionValue.getName()).getType()) {
          optionsMap.put(optionValue.getName(), optionValue);
        } else {
          resultList.add(optionValue);
        }
      }
    }
    resultList.addAll(optionsMap.values());
    return resultList.iterator();
  }

  @Override
  protected boolean supportsOptionType(OptionValue.OptionType type) {
    return true;
  }

  /**
   * If atleast one of underlying optionManagers has accepted to add listener, then its a success.
   * Otherwise an exception is thrown.
   *
   * @param optionChangeListener
   * @throws UnsupportedOperationException
   */
  @Override
  public void addOptionChangeListener(OptionChangeListener optionChangeListener) throws UnsupportedOperationException {
    boolean atleastOneSuccess = false;
    UnsupportedOperationException exception = null;

    for(OptionManager om: optionManagers) {
      try {
         om.addOptionChangeListener(optionChangeListener);
         atleastOneSuccess = true;
      } catch(UnsupportedOperationException e) {
        exception = e;
      }
    }

    if (!atleastOneSuccess && exception != null) {
      throw exception;
    }
  }
}
