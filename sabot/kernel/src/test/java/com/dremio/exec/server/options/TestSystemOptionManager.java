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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProto;
import com.dremio.test.DremioTest;
import com.google.common.collect.Iterators;

/**
 * Tests for {@link SystemOptionManager}
 */
public class TestSystemOptionManager extends DremioTest {

  private LegacyKVStoreProvider kvStoreProvider;
  private LogicalPlanPersistence lpp;
  private SabotConfig sabotConfig;

  @Before
  public void before() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(
      DremioTest.CLASSPATH_SCAN_RESULT, null, true, false
    ).asLegacy();
    kvStoreProvider.start();
    sabotConfig = SabotConfig.create();
    lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
  }

  @Test
  public void testMigration() throws Exception {
    final LegacyKVStore<String, OptionValueProto> kvStore = kvStoreProvider.getStore(
      SystemOptionManager.OptionProtoStoreCreator.class);

    final DefaultOptionManager dom = new DefaultOptionManager(CLASSPATH_SCAN_RESULT);
    final SystemOptionManager som = new SystemOptionManager(dom, lpp, () -> kvStoreProvider, false);

    // Persistent Store to directly add legacy options
    final OptionValueStore persistentStore = new OptionValueStore(
      () -> kvStoreProvider,
      SystemOptionManager.OptionStoreCreator.class,
      new JacksonSerializer<>(lpp.getMapper(), OptionValue.class)
    );
    persistentStore.start();

    // put some options with old pojo/jackson serialized format
    final OptionValue option1 = OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM, "planner.disable_exchanges", true);
    persistentStore.put("planner.disable_exchanges", option1);

    final OptionValue option2 = OptionValue.createLong(
      OptionValue.OptionType.SYSTEM, "planner.partitioner_sender_threads_factor", 4);
    persistentStore.put("planner.partitioner_sender_threads_factor", option2);

    // migrate
    som.start();

    // check options are migrated
    assertNull(persistentStore.get(option1.getName()));
    assertNull(persistentStore.get(option2.getName()));

    // test option retrieval via option manager
    final OptionValue retrievedOption1 = som.getOption(option1.getName());
    assertEquals(retrievedOption1, option1);
    final OptionValue retrievedOption2 = som.getOption(option2.getName());
    assertEquals(retrievedOption2, option2);

    // test kvStore value matches
    assertEquals(option1, OptionValueProtoUtils.toOptionValue(kvStore.get(option1.getName())));
    assertEquals(option2, OptionValueProtoUtils.toOptionValue(kvStore.get(option2.getName())));

    // test size of stores
    assertEquals(0, getNumOptions(persistentStore)); // old store should have no options
    assertTrue(getNumOptions(kvStore) >= 2); // new store should have at least 2
  }

  private int getNumOptions(OptionValueStore persistentStore) {
    return Iterators.size(persistentStore.getAll());
  }

  private int getNumOptions(LegacyKVStore kvStore) {
    return Iterators.size(kvStore.find().iterator());
  }
}
