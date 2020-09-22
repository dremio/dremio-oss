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
package com.dremio.telemetry.api.config;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.inject.Provider;

/**
 * Template for creating auto refreshing configs. Users listen to config changes by implementing
 * a configRefreshListener<T>. As the refresh settings change, they are automatically applied.
 * If the provider cannot provide any initial config, then the auto refresher auto refreshes at a default
 * interval until a file is picked up.
 * @param <T> The type of the root level config that sits beside refreshConfigurator.
 */
public class AutoRefreshConfigurator<T> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AutoRefreshConfigurator.class);
  private static final long DEFAULT_MINIMUM_REFRESH_FREQUENCY = TimeUnit.SECONDS.toMillis(90);

  private final Provider<CompleteRefreshConfig<T>> getter;
  private final long minRefreshIntervalMS;

  private volatile boolean refreshEnabled = true;
  private volatile long refreshIntervalMS;
  private volatile ValueChangeDetector<T> trigger;
  private volatile Thread refreshThread;


  public AutoRefreshConfigurator(Provider<CompleteRefreshConfig<T>> getter, Consumer<T> listener) {
    this(getter, listener, DEFAULT_MINIMUM_REFRESH_FREQUENCY);
  }

  public AutoRefreshConfigurator(Provider<CompleteRefreshConfig<T>> getter, Consumer<T> listener, long minRefreshIntervalMS) {
    this.getter = getter;
    this.minRefreshIntervalMS = minRefreshIntervalMS;
    // In case the first read is bad.
    this.refreshIntervalMS = this.minRefreshIntervalMS;
    trigger = new ValueChangeDetector<>(listener::accept);

    refreshOnce();

    refreshThread = new Thread(this::refreshContinually, "config-refresh");
    refreshThread.setDaemon(true);
    refreshThread.start();
  }

  private void refreshContinually() {
    try {
      while(refreshEnabled) {
        Thread.sleep(refreshIntervalMS);
        refreshOnce();
      }
    } catch (InterruptedException ex) {
      logger.info("Refresh thread interrupted, exiting.", ex);
      return;
    }
  }

  private void refreshOnce() {
    CompleteRefreshConfig<T> newState = getter.get();

    // There could've been an error reading the file. Do nothing.
    if (newState == null) {
      return;
    }

    final RefreshConfiguration refreshConf = newState.getRefreshConfiguration();
    if (refreshConf != null) {
      refreshEnabled = refreshConf.isEnabled();
      final long proposedRefreshMs = refreshConf.getIntervalMS();

      if(proposedRefreshMs < minRefreshIntervalMS) {
        logger.warn("Requested configuration refresh frequency {}ms. Adjusting to minimum of {}ms.", proposedRefreshMs, minRefreshIntervalMS);
        refreshIntervalMS = minRefreshIntervalMS;
      } else {
        refreshIntervalMS = proposedRefreshMs;
      }
    } else {
      logger.warn("Could not detect refresh settings. Continuing to refresh at {}s intervals.", TimeUnit.MILLISECONDS.toSeconds(refreshIntervalMS));
    }

    trigger.checkNewValue(newState.getUserConfig());
  }

  /**
   * User config bundled with refresh config.
   * @param <T> user config type
   */
  public static class CompleteRefreshConfig<T> {
    private final RefreshConfiguration refreshConfiguration;
    private final T userConfig;

    public CompleteRefreshConfig(RefreshConfiguration refreshConfiguration, T userConfig) {
      this.refreshConfiguration = refreshConfiguration;
      this.userConfig = userConfig;
    }

    public T getUserConfig() {
      return userConfig;
    }

    public RefreshConfiguration getRefreshConfiguration() {
      return refreshConfiguration;
    }
  }

  /**
   * Provides a generic way to run a reaction when a value changes.
   * Treats null as a unique instance.
   *
   * Consider a series of values:
   * null 1 1 1 null null 2 2 2
   *      ^     ^         ^
   * The reaction will be run at the ^ places.
   * The initial "remembered" value is null. Currently not configurable.
   * Although, I supposed it could be DI.
   * @param <T> The type of the value you want to detect changes on. Must implement hashCode.
   */
  public static class ValueChangeDetector<T> {
    private boolean prevIsNull = true;
    private T lastValue;
    private final Consumer<T> reaction;

    public ValueChangeDetector(Consumer<T> reaction) {
      this.reaction = reaction;
    }

    public void checkNewValue(T newVal) {
      if (newVal == null) {
        if (!prevIsNull) {
          reaction.accept(null);
          prevIsNull = true;
        }
        return;
      }

      final int h = newVal.hashCode();
      if (!newVal.equals(lastValue) || prevIsNull) {
        lastValue = newVal;
        prevIsNull = false;
        reaction.accept(newVal);
      }
    }

    public T getLastValue() {
      return lastValue;
    }

  }
}
