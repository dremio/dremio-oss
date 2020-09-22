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

import org.junit.Before;
import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProto;
import com.dremio.test.DremioTest;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * Tests for legacy store migration in {@link SystemOptionManager}
 */
public class TestSystemOptionManagerMigration extends DremioTest {

  private LegacyKVStoreProvider kvStoreProvider;
  private LogicalPlanPersistence lpp;
  private SabotConfig sabotConfig;
  private OptionValidatorListingImpl optionValidatorListing;
  private SystemOptionManager som;

  @Before
  public void before() throws Exception {
    kvStoreProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();
    sabotConfig = SabotConfig.create();
    lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
    optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    som = new SystemOptionManager(optionValidatorListing, lpp, () -> kvStoreProvider, false);
  }

  @Test
  public void testMigrationFromJson() throws Exception {
    // Legacy Store to directly add legacy options
    final OptionValueStore legacyStore = new OptionValueStore(
      () -> kvStoreProvider,
      SystemOptionManager.LegacyJacksonOptionStoreCreator.class,
      new JacksonSerializer<>(lpp.getMapper(), OptionValue.class)
    );
    legacyStore.start();

    // put some options with old jackson serialized format
    final OptionValue option1 = OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM, "planner.disable_exchanges", true);
    legacyStore.put("Planner.Disable_Exchanges", option1); // check legacy capitalization doesn't break

    final OptionValue option2 = OptionValue.createLong(
      OptionValue.OptionType.SYSTEM, "planner.partitioner_sender_threads_factor", 4);
    legacyStore.put("planner.partitioner_sender_threads_factor", option2);

    // migrate
    som.start();

    // check options are deleted from legacy store
    assertEquals(0, getNumOptions(legacyStore)); // old store should have no options
    assertNull(legacyStore.get(option1.getName()));
    assertNull(legacyStore.get(option2.getName()));

    // check options match
    assertEquals(som.getOption(option1.getName()), option1);
    assertEquals(som.getOption(option2.getName()), option2);
  }

  @Test
  public void testMigrationFromProto() throws Exception {
    // Legacy Store to directly add legacy options
    final LegacyKVStore<String, OptionValueProto> legacyStore = kvStoreProvider.getStore(
      SystemOptionManager.LegacyProtoOptionStoreCreator.class);

    // put some options with old protobuf format
    final OptionValue option1 = OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM, "planner.disable_exchanges", true);
    legacyStore.put("Planner.Disable_Exchanges", OptionValueProtoUtils.toOptionValueProto(option1)); // check legacy capitalization doesn't break

    final OptionValue option2 = OptionValue.createLong(
      OptionValue.OptionType.SYSTEM, "planner.partitioner_sender_threads_factor", 4);
    legacyStore.put("planner.partitioner_sender_threads_factor", OptionValueProtoUtils.toOptionValueProto(option2));

    // migrate
    som.start();

    // check options are deleted from legacy store
    assertEquals(0, getNumOptions(legacyStore)); // old store should have no options
    assertNull(legacyStore.get(option1.getName()));
    assertNull(legacyStore.get(option2.getName()));

    // check options match
    assertEquals(som.getOption(option1.getName()), option1);
    assertEquals(som.getOption(option2.getName()), option2);
  }

  private int getNumOptions(OptionValueStore persistentStore) {
    return Iterables.size(persistentStore.getAll());
  }

  private int getNumOptions(LegacyKVStore kvStore) {
    return Iterators.size(kvStore.find().iterator());
  }
}
