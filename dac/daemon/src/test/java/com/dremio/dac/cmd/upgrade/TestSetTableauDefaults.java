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
package com.dremio.dac.cmd.upgrade;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.Version;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.dac.resource.TableauResource;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.test.DremioTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test the upgrade task for setting Tableau defaults based on version. */
public class TestSetTableauDefaults extends DremioTest {

  private SystemOptionManager optionManager;
  private LegacyKVStoreProvider kvStoreProvider;

  @Before
  public void setUp() throws Exception {
    final SabotConfig sabotConfig = SabotConfig.create();
    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    if (kvStoreProvider == null) {
      kvStoreProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
      kvStoreProvider.start();
    }

    optionManager =
        new SystemOptionManager(
            new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT),
            lpp,
            DirectProvider.wrap(kvStoreProvider),
            false);
    optionManager.start();
  }

  @After
  public void cleanUp() throws Exception {
    kvStoreProvider.close();
    kvStoreProvider = null;
    optionManager.close();
    optionManager = null;
  }

  @Test
  public void testPre4x0Version() throws Exception {
    final Version version = new Version("3.9.0", 3, 9, 0, 0, "");
    assertTrue(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
    assertFalse(optionManager.getOption(TableauResource.CLIENT_TOOLS_TABLEAU));
  }

  @Test
  public void testPre460Version() throws Exception {
    final Version version = new Version("4.5.9", 4, 5, 9, 0, "");
    assertTrue(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
    assertFalse(optionManager.getOption(TableauResource.CLIENT_TOOLS_TABLEAU));
  }

  @Test
  public void test460Version() throws Exception {
    final Version version = new Version("4.6.0", 4, 6, 0, 0, "");
    assertFalse(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
  }

  @Test
  public void testPost460Version() throws Exception {
    final Version version = new Version("4.6.1", 4, 6, 1, 0, "");
    assertFalse(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
  }

  @Test
  public void testPost4x0Version() throws Exception {
    final Version version = new Version("5.2.0", 5, 2, 0, 0, "");
    assertFalse(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
  }

  @Test
  public void testPre460VersionAndKeyExists() throws Exception {
    final Version version = new Version("5.1.0", 5, 2, 0, 0, "");
    assertFalse(SetTableauDefaults.updateOptionsIfNeeded(version, () -> optionManager, true));
    assertFalse(optionManager.isSet(TableauResource.CLIENT_TOOLS_TABLEAU.getOptionName()));
  }
}
