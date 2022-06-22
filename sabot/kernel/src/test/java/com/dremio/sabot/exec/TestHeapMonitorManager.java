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

package com.dremio.sabot.exec;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProtoList;
import com.dremio.options.TypeValidators.AdminBooleanValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestHeapMonitorManager {
  private SystemOptionManager setupSystemOptionManager() throws Exception {
    LegacyKVStore<String, OptionValueProtoList> kvStore;
    OptionValidatorListing optionValidatorListing;
    optionValidatorListing = spy(new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT));
    LogicalPlanPersistence lpp = mock(LogicalPlanPersistence.class);
    when(lpp.getMapper()).thenReturn(mock(ObjectMapper.class));

    kvStore = mock(LegacyKVStore.class);
    LegacyKVStoreProvider storeProvider = mock(LegacyKVStoreProvider.class);
    when(storeProvider.getStore(SystemOptionManager.OptionStoreCreator.class))
      .thenReturn(kvStore);

    LegacyKVStore mockedEmptyKVStore = mock(LegacyKVStore.class);

    when(mockedEmptyKVStore.find()).thenReturn(Collections.emptyList());
    when(storeProvider.getStore(SystemOptionManager.LegacyProtoOptionStoreCreator.class))
      .thenReturn(mockedEmptyKVStore);
    when(storeProvider.getStore(SystemOptionManager.LegacyJacksonOptionStoreCreator.class))
      .thenReturn(mockedEmptyKVStore);

    SystemOptionManager som = spy(new SystemOptionManager(optionValidatorListing, lpp, () -> storeProvider, false));
    som.start();
    reset(kvStore); //clearInvocations
    return som;
  }

  @Test
  public void testCoordinatorHeapMonitorEnabledDisabled() throws Exception {
    Consumer<CancelQueryContext> noOpCancelConsumer = (cancelQueryContext) -> {};
    HeapClawBackStrategy heapClawBackStrategy = new CoordinatorHeapClawBackStrategy(noOpCancelConsumer);
    testHeapMonitorEnabledDisabled(heapClawBackStrategy,
                                   ExecConstants.COORDINATOR_ENABLE_HEAP_MONITORING,
                                   ExecConstants.COORDINATOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE,
                                   ExecConstants.COORDINATOR_HEAP_MONITOR_DELAY_MILLIS,
                                   Role.COORDINATOR);
  }

  @Test
  public void testExecutorHeapMonitorEnabledDisabled() throws Exception {
    FragmentExecutors fragmentExecutors = mock(FragmentExecutors.class);
    QueriesClerk queriesClerk = mock(QueriesClerk.class);
    HeapClawBackStrategy heapClawBackStrategy = new FailGreediestQueriesStrategy(fragmentExecutors, queriesClerk);
    testHeapMonitorEnabledDisabled(heapClawBackStrategy,
                                   ExecConstants.EXECUTOR_ENABLE_HEAP_MONITORING,
                                   ExecConstants.EXECUTOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE,
                                   ExecConstants.EXECUTOR_HEAP_MONITOR_DELAY_MILLIS,
                                   Role.EXECUTOR);
  }

  private void testHeapMonitorEnabledDisabled(HeapClawBackStrategy heapClawBackStrategy,
                                              AdminBooleanValidator enableHeapMonitoring,
                                              RangeLongValidator clawbackThreshPercentage,
                                              RangeLongValidator delayHeapMonitorMillis,
                                              Role role) throws Exception {
    OptionManager som = setupSystemOptionManager();

    // First enable heap monitoring and verify heapMonitoringThread is running.
    doReturn(true).when(som).getOption(enableHeapMonitoring);
    doReturn(85L).when(som).getOption(clawbackThreshPercentage);
    doReturn(2_000L).when(som).getOption(delayHeapMonitorMillis);
    HeapMonitorManager heapMonitorManager = new HeapMonitorManager(() -> som,
                                                                   heapClawBackStrategy,
                                                                   role);
    heapMonitorManager.start();
    assertTrue("Heap monitor should be running.", heapMonitorManager.isHeapMonitorThreadRunning());

    // Disable heap monitoring and verify that heapMonitoringThread is NOT running.
    doReturn(false).when(som).getOption(enableHeapMonitoring);
    // DeleteAllOptions is just to simulate change in option,
    // so that HeapMonitorManager.HeapOptionChangeListener.onChange() is triggered
    som.deleteAllOptions(OptionValue.OptionType.SYSTEM);
    assertTrue("Heap monitor should not be running.", !heapMonitorManager.isHeapMonitorThreadRunning());

    // Enable back heap monitoring and verify that heapMonitoringThread is running.
    doReturn(true).when(som).getOption(enableHeapMonitoring);
    // DeleteAllOptions is just to simulate change in option,
    // so that HeapMonitorManager.HeapOptionChangeListener.onChange() is triggered
    som.deleteAllOptions(OptionValue.OptionType.SYSTEM);
    assertTrue("Heap monitor should be running.", heapMonitorManager.isHeapMonitorThreadRunning());
    heapMonitorManager.close();
  }
}
