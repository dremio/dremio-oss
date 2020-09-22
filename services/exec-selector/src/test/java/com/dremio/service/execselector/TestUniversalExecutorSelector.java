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
package com.dremio.service.execselector;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.google.common.collect.ImmutableSet;

/**
 * Unit test for the {@link UniversalExecutorSelector}
 */
public class TestUniversalExecutorSelector {
  private ClusterCoordinator clusterCoordinator;
  private TestServiceSet serviceSet;
  private OptionManager optionManager;
  private ExecutorSelectionService selectionService;

  @Before
  public void setup() throws Exception {
    clusterCoordinator = mock(ClusterCoordinator.class);
    serviceSet = new TestServiceSet();
    when(clusterCoordinator.getServiceSet(any())).thenReturn(serviceSet);

    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("universal");

    final ExecutorSelectorFactory executorSelectorFactory = new ExecutorSelectorFactoryImpl();

    Provider<OptionManager> optionManagerProvider = () -> optionManager;
    selectionService = new ExecutorSelectionServiceImpl(
        () -> new LocalExecutorSetService(() -> clusterCoordinator,
                                          optionManagerProvider),
        optionManagerProvider,
        () -> executorSelectorFactory,
        new ExecutorSelectorProvider());
    selectionService.start();
  }

  @After
  public void cleanup() throws Exception {
    selectionService.close();
  }

  @Test
  public void testAddRemoveNode() throws Exception {
    // Initial state in the service: no nodes registered
    TestExecutorSelectorUtil.checkExecutors(selectionService, 0, ImmutableSet.of());

    // Start populating nodes, make sure we get them back
    serviceSet.testAddNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("one"));
    serviceSet.testAddNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("one", "two"));
    serviceSet.testRemoveNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("two"));
  }
}
