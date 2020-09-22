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
package com.dremio.dac.server.tokens;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.test.DremioTest;

/**
 * Test token management.
 */
public class TestTokenManager {

  private static TokenManagerImpl manager;
  private static LegacyKVStoreProvider provider;

  private static final String username = "testuser";
  private static final String clientAddress = "localhost";

  @BeforeClass
  public static void startServices() throws Exception {
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionValidatorProvider(mock(OptionValidatorListingImpl.class))
      .withOptionManager(mock(DefaultOptionManager.class))
      .withOptionManager(mock(SystemOptionManager.class))
      .build();
    when(optionManager.getOption("token.expiration.min")).thenReturn(OptionValue.createOption
      (OptionValue.Kind.LONG, OptionValue.OptionType.SYSTEM, "token.expiration.min","30"));
    when(optionManager.getOption("token.release.leadership.ms")).thenReturn(OptionValue.createOption
      (OptionValue.Kind.LONG, OptionValue.OptionType.SYSTEM, "token.release.leadership.ms","144000000"));

    provider =
        LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    provider.start();
    manager = new TokenManagerImpl(
      new Provider<LegacyKVStoreProvider>() {
        @Override
        public LegacyKVStoreProvider get() {
          return provider;
        }
      },
      new Provider<SchedulerService>() {
        @Override
        public SchedulerService get() {
          return mock(SchedulerService.class);
        }
      },
      new Provider<OptionManager>() {
        @Override
        public OptionManager get() {
          return optionManager;
        }
      },
    false, 10, 10);
    manager.start();
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (provider != null) {
      provider.close();
    }
    if (manager != null) {
      manager.close();
    }
  }

  @Test
  public void validToken() throws Exception {
    final TokenDetails details = manager.createToken(username, clientAddress);
    assertEquals(manager.validateToken(details.token).username, username);
    assertTrue(manager.getTokenStore().get(details.token) != null);
  }

  @Test
  public void nullToken() throws Exception {
    try {
      manager.validateToken(null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "invalid token");
    }
  }

  @Test
  public void invalidToken() throws Exception {
    try {
      manager.validateToken("hopethistokenisnevergenerated");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "invalid token");
    }
  }

  @Test
  public void useTokenAfterExpiry() throws Exception {
    final long now = System.currentTimeMillis();
    final long expires = now + 1000;

    final TokenDetails details = manager.createToken(username, clientAddress, now, expires);
    assertEquals(manager.validateToken(details.token).username, username);
    assertTrue(manager.getTokenStore().get(details.token) != null);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      fail("I was sleeping!");
    }

    try {
      manager.validateToken(details.token);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "token expired");
    }

    assertTrue(manager.getTokenStore().get(details.token) == null);
  }

  @Test
  public void useTokenAfterLogOut() throws Exception {
    final long now = System.currentTimeMillis();
    final long expires = now + (10 * 1000);

    final TokenDetails details = manager.createToken(username, clientAddress, now, expires);
    assertEquals(manager.validateToken(details.token).username, username);
    assertTrue(manager.getTokenStore().get(details.token) != null);

    manager.invalidateToken(details.token); // ..logout

    try {
      manager.validateToken(details.token);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "invalid token");
    }

    assertTrue(manager.getTokenStore().get(details.token) == null);
  }

  @Test
  public void checkExpiration() throws Exception {
    final long now = System.currentTimeMillis();
    final long expires = now + (10 * 1000);

    final TokenDetails details = manager.createToken(username, clientAddress, now, expires);
    assertEquals((long) manager.getTokenStore().get(details.token).getExpiresAt(), expires);
  }
}
