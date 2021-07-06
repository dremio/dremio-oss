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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.Version;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.dac.explore.bi.TableauMessageBodyGenerator;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.test.DremioTest;

/**
 * Test the upgrade task for setting Tableau export defaults based on version.
 */
public class TestSetExportType extends DremioTest {

  private SystemOptionManager optionManager;
  private LegacyKVStoreProvider kvStoreProvider;

  @Before
  public void setUp() throws Exception {
    final SabotConfig sabotConfig = SabotConfig.create();
    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
    if (kvStoreProvider == null) {
      kvStoreProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
      kvStoreProvider.start();
    }

    optionManager = new SystemOptionManager(new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT),
      lpp, DirectProvider.wrap(kvStoreProvider), false);
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
  public void test1600Version() throws Exception {
    final Version version = new Version("16.0.0", 16, 0, 0, 0, "");
    assertFalse(SetExportType.updateOptionsIfNeeded(version, () -> optionManager, true));
  }

  @Test
  public void testPost1600Version() throws Exception {
    final Version version = new Version("17.0.0", 17, 0, 0, 0, "");
    assertFalse(SetExportType.updateOptionsIfNeeded(version, () -> optionManager, true));
  }

  @Test
  public void testPre1600Version() throws Exception {
    final Version version = new Version("15.0.0", 15, 0, 0, 0, "");
    assertTrue(SetExportType.updateOptionsIfNeeded(version, () -> optionManager, true));
    assertEquals("odbc", optionManager.getOption(TableauMessageBodyGenerator.TABLEAU_EXPORT_TYPE));
  }
}
