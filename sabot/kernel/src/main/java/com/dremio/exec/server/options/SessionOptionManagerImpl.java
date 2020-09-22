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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.google.common.base.Predicate;

/**
 * {@link OptionManager} that holds options within {@link com.dremio.sabot.rpc.user.UserSession} context. Options
 * set at the session level only apply to queries that you run during the current Dremio connection. Session level
 * settings override system level settings.
 *
 * NOTE that currently, the effects of deleting a short lived option (see {@link OptionValidator#isShortLived}) are
 * undefined. For example, we inject an exception (passed through an option), then try to delete the option, depending
 * on where the exception was injected, the reset query could either succeed or the exception could actually be thrown
 * in the reset query itself.
 */
public class SessionOptionManagerImpl extends InMemoryOptionManager implements SessionOptionManager {
  private final OptionValidatorListing optionValidatorListing;
  private final AtomicInteger queryCount = new AtomicInteger(0);

  /**
   * Map of short lived options. Key: option name, Value: [ start, end )
   */
  private final Map<String, ImmutablePair<Integer, Integer>> shortLivedOptions =
    CaseInsensitiveMap.newConcurrentMap();

  public SessionOptionManagerImpl(OptionValidatorListing optionValidatorListing) {
    super(optionValidatorListing, CaseInsensitiveMap.newConcurrentMap());
    this.optionValidatorListing = optionValidatorListing;
  }

  @Override
  public boolean setOption(final OptionValue value) {
    optionValidatorListing.getValidator(value.getName()).validate(value);
    if (!super.setOption(value)) {
      return false;
    }
    final String name = value.getName();
    final OptionValidator validator = optionValidatorListing.getValidator(name);
    final boolean shortLived = validator.isShortLived();
    if (shortLived) {
      final int start = queryCount.get() + 1; // start from the next query
      final int ttl = validator.getTtl();
      final int end = start + ttl;
      shortLivedOptions.put(name, new ImmutablePair<>(start, end));
    }
    return true;
  }

  @Override
  public OptionValue getOption(final String name) {
    final OptionValue value = super.getOption(name);
    if (shortLivedOptions.containsKey(name)) {
      if (withinRange(name)) {
        return value;
      }
      final int queryNumber = queryCount.get();
      final int start = shortLivedOptions.get(name).getLeft();
      // option is not in effect if queryNumber < start
      if (queryNumber < start) {
        return optionValidatorListing.getValidator(name).getDefault();
      // reset if queryNumber <= end
      } else {
        options.remove(name);
        shortLivedOptions.remove(name);
        return null; // fallback takes effect
      }
    }
    return value;
  }

  private boolean withinRange(final String name) {
    final int queryNumber = queryCount.get();
    final ImmutablePair<Integer, Integer> pair = shortLivedOptions.get(name);
    final int start = pair.getLeft();
    final int end = pair.getRight();
    return start <= queryNumber && queryNumber < end;
  }

  private final Predicate<OptionValue> isLive = new Predicate<OptionValue>() {
    @Override
    public boolean apply(final OptionValue value) {
      final String name = value.getName();
      return !shortLivedOptions.containsKey(name) || withinRange(name);
    }
  };

  @Override
  protected boolean supportsOptionType(OptionType type) {
    return type == OptionType.SESSION;
  }

  @Override
  public void incrementQueryCount() {
    queryCount.incrementAndGet();
  }
}
