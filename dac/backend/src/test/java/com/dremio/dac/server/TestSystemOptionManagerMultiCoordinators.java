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
package com.dremio.dac.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionValue;


/**
 * Test changing a simple system option to be synced on multiple coordinators
 **/
public class TestSystemOptionManagerMultiCoordinators extends BaseTestServer {
  private SystemOptionManager somMaster;
  private SystemOptionManager somNonMaster;

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeTrue(isMultinode());
    BaseTestServer.init();
  }

  @Before
  public void setUp() throws Exception {
    clearAllDataExceptUser();
    somMaster = getMasterDremioDaemon().getBindingProvider().lookup(SystemOptionManager.class);
    somNonMaster = getCurrentDremioDaemon().getBindingProvider().lookup(SystemOptionManager.class);
  }

  @Test
  public void testOptionMultiCoordinators() throws Exception {

    // Set an option with non-default value on Master and then validate the value in both Master and nonMaster
    OptionValue ov = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "store.plugin.max_metadata_leaf_columns", 1000);
    somMaster.setOption(ov);
    assertEquals(ov.getNumVal(), somMaster.getOption("store.plugin.max_metadata_leaf_columns").getNumVal());
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    assertEquals(ov.getNumVal(), somNonMaster.getOption("store.plugin.max_metadata_leaf_columns").getNumVal());

    // Set an existing non-default option with default value on Master and then validate the value in both Master and nonMaster
    OptionValue ov1 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "store.plugin.max_metadata_leaf_columns", 800);
    somMaster.setOption(ov1);
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    assertEquals(ov1.getNumVal(), somNonMaster.getOption("store.plugin.max_metadata_leaf_columns").getNumVal());


    // Set one option on Master and then validate the value
    ov = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.cross_source_select.disable", true);
    somMaster.setOption(ov);
    assertEquals(ov.getBoolVal(), somMaster.getOption("planner.cross_source_select.disable").getBoolVal());
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    assertEquals(ov.getBoolVal(), somNonMaster.getOption("planner.cross_source_select.disable").getBoolVal());

    // set another option on Master and then validate the value
    ov1 = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "store.plugin.keep_metadata_on_replace", true);
    somMaster.setOption(ov1);
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    assertEquals(ov1.getBoolVal(), somNonMaster.getOption("store.plugin.keep_metadata_on_replace").getBoolVal());

    // delete one option on Master and then validate the deleted one and the non-deleted one
    somMaster.deleteOption("store.plugin.keep_metadata_on_replace", OptionValue.OptionType.SYSTEM);
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    OptionValue ov2 = somNonMaster.getOption("store.plugin.keep_metadata_on_replace");
    assertNull(ov2);
    assertEquals(ov.getBoolVal(), somNonMaster.getOption("planner.cross_source_select.disable").getBoolVal());

    // deleteAll on Master and then validate the deleted one
    somMaster.deleteAllOptions(OptionValue.OptionType.SYSTEM);
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    ov2 = somNonMaster.getOption("planner.cross_source_select.disable");
    assertNull(ov2);


    // set one option on Master, clear nonMaster cache, repopulate at nonMaster,  and validate the option value
    ov = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "store.plugin.max_nested_levels", 32);
    somMaster.setOption(ov);
    assertEquals(ov.getNumVal(), somMaster.getOption("store.plugin.max_nested_levels").getNumVal());
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    somNonMaster.clearCachedOptionProtoList();
    somNonMaster.populateCache();
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(50));
    assertEquals(ov.getNumVal(), somNonMaster.getOption("store.plugin.max_nested_levels").getNumVal());
  }

}

