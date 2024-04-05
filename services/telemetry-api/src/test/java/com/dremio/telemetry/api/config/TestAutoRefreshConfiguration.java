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

import static org.junit.Assert.assertEquals;

import com.dremio.telemetry.api.config.AutoRefreshConfigurator.CompleteRefreshConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Provider;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensure that AutoRefreshConfigurator appropriately picks up new config and passes it to the
 * listener when unique.
 */
public class TestAutoRefreshConfiguration {

  /**
   * Listens for changes to our test config. Our test config just contains an Integer and Auto
   * refresh settings.
   */
  private final class TestConfListener implements Consumer<Integer> {
    private volatile List<Integer> log = new ArrayList<>();

    @Override
    public void accept(Integer newInt) {
      log.add(newInt);
    }

    public List<Integer> getLog() {
      return log;
    }
  }

  private final long defaultRefreshInterval = 50;
  private final RefreshConfiguration defaultRefreshEnabled =
      new RefreshConfiguration(true, defaultRefreshInterval, TimeUnit.MILLISECONDS);
  private final RefreshConfiguration disabledRefreshConf =
      new RefreshConfiguration(false, defaultRefreshInterval, TimeUnit.MILLISECONDS);

  private TestConfListener listener;

  public void defaultSleep() throws InterruptedException {
    // Sleep with a little cushion to make sure we are not passing on race conditions.
    Thread.sleep(defaultRefreshInterval * 3);
  }

  @Before
  public void setup() {
    listener = new TestConfListener();
  }

  public CompleteRefreshConfig<Integer> makeDefaultConfig(Integer val) {
    return new CompleteRefreshConfig<>(defaultRefreshEnabled, val);
  }

  @Test
  public void testInitialStateNoChanges() throws InterruptedException {
    final Integer staleInt = 42;
    final CompleteRefreshConfig<Integer> wholeConf =
        new CompleteRefreshConfig<>(defaultRefreshEnabled, staleInt);

    Provider<CompleteRefreshConfig<Integer>> unchangingGetter = () -> wholeConf;

    AutoRefreshConfigurator<Integer> refresher =
        new AutoRefreshConfigurator<>(unchangingGetter, listener, 0);

    defaultSleep();

    assertEquals(1, listener.getLog().size());
    assertEquals(42, listener.getLog().get(0).intValue());
  }

  @Test
  public void testBadInitialRead() throws InterruptedException {
    final Integer staleInt = null;
    Provider<CompleteRefreshConfig<Integer>> evolvingGetter =
        makeDynamicConfig(
            Arrays.asList(null, new CompleteRefreshConfig<Integer>(disabledRefreshConf, 13)));
    AutoRefreshConfigurator<Integer> refresher =
        new AutoRefreshConfigurator<>(evolvingGetter, listener, defaultRefreshInterval);

    defaultSleep();

    assertEquals(1, listener.getLog().size());
    assertEquals(listener.getLog().get(0).intValue(), 13);
  }

  @Test
  public void testChangingConfig() throws InterruptedException {
    Provider<CompleteRefreshConfig<Integer>> evolvingGetter =
        makeDynamicConfig(
            Arrays.asList(
                makeDefaultConfig(0),
                makeDefaultConfig(-1),
                makeDefaultConfig(
                    null), // Should not register because null isn't a valid return value for
                // config.
                makeDefaultConfig(-1),
                makeDefaultConfig(-1),
                null, // Should continue refreshing despite null because of present settings.
                new CompleteRefreshConfig<Integer>(disabledRefreshConf, 42)));

    AutoRefreshConfigurator<Integer> refresher =
        new AutoRefreshConfigurator<>(evolvingGetter, listener, 0);

    // We should go through 6 refresh cycles - extra cushion to avoid flakiness.
    Thread.sleep(defaultRefreshInterval * 8);

    assertEquals(listener.log, Arrays.asList(0, -1, null, -1, 42));
  }

  private Provider<CompleteRefreshConfig<Integer>> makeDynamicConfig(
      List<CompleteRefreshConfig<Integer>> results) {

    Provider<CompleteRefreshConfig<Integer>> evolvingGetter =
        new Provider<CompleteRefreshConfig<Integer>>() {
          private int onGet = 1;

          @Override
          public CompleteRefreshConfig<Integer> get() {
            if (results.size() >= onGet) {
              final CompleteRefreshConfig<Integer> ret = results.get(onGet - 1);
              ++onGet;
              return ret;
            }
            return null;
          }
        };

    return evolvingGetter;
  }
}
