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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.google.common.collect.ImmutableSet;

/**
 *
 */
public class TestExecutorSelectionService {
  private ClusterCoordinator clusterCoordinator;
  private TestExecutorSelectionServiceSet serviceSet;
  private OptionManager optionManager;
  private ExecutorSelectorProvider executorSelectorProvider;
  private ExecutorSelectionService selectionService;
  private ExecutorSetService executorSetService;
  private boolean versionCheckEnabled = true;

  public void simulateRestartCoordinator(boolean versionCheckEnabled) throws Exception {
    this.versionCheckEnabled = versionCheckEnabled;
    setup();
  }

  @Before
  public void setup() throws Exception {
    clusterCoordinator = mock(ClusterCoordinator.class);
    serviceSet = new TestExecutorSelectionServiceSet();
    when(clusterCoordinator.getServiceSet(any())).thenReturn(serviceSet);

    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("default");
    when(optionManager.getOption(eq(ExecutorSetService.DREMIO_VERSION_CHECK))).thenReturn(versionCheckEnabled);

    final ExecutorSelectorFactory executorSelectorFactory = new TestExecutorSelectorFactory();
    executorSelectorProvider = new ExecutorSelectorProvider();
    Provider<OptionManager> optionManagerProvider = () -> optionManager;
    executorSetService = new LocalExecutorSetService(() -> clusterCoordinator,
                                                     optionManagerProvider);
    selectionService = new ExecutorSelectionServiceImpl(
        () -> executorSetService,
        optionManagerProvider,
        () -> executorSelectorFactory,
        executorSelectorProvider);
    selectionService.start();
  }

  @After
  public void cleanup() throws Exception {
    selectionService.close();
    executorSetService.close();
  }

  // A mock executor selector
  // Always initially created with a single node -- contains the "name" of the executor selector (used to test config changes)
  // Otherwise, blindly returns the set of nodes that are registed with it.
  private static class TestExecutorSelector implements ExecutorSelector {
    private final Set<NodeEndpoint> executors;
    private final String selectorName;

    TestExecutorSelector(String selectorName) {
      this.executors = new HashSet<>();
      this.selectorName = selectorName;
      executors.add(NodeEndpoint.newBuilder()
        .setAddress(selectorName)
        .build());
    }

    @Override
    public ExecutorSelectionHandle getExecutors(int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext) {
      return new ExecutorSelectionHandleImpl(executors);
    }

    @Override
    public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
      executors.removeAll(unregisteredNodes);
    }

    @Override
    public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
      executors.addAll(registeredNodes);
    }

    @Override
    public int getNumExecutors() {
      return executors.size();
    }

    String getSelectorName() {
      return selectorName;
    }

    @Override
    public void close() {
    }
  }

  private static class TestExecutorSelectorFactory implements ExecutorSelectorFactory {
    @Override
    public ExecutorSelector createExecutorSelector(String selectorType, ReentrantReadWriteLock rwLock) {
      return new TestExecutorSelector(selectorType);
    }
  }

  @Test
  public void testSwitchImpl() throws Exception {
    assertEquals("default", ((TestExecutorSelector)executorSelectorProvider.get()).getSelectorName());

    // Switch to mode "a", expect created nodes to be "a"-nodes
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("aNode");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("aNode"));
    assertEquals("aNode", ((TestExecutorSelector)executorSelectorProvider.get()).getSelectorName());

    // Switch to mode "b", expect created nodes to be "b"-nodes
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("bNode");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("bNode"));
    assertEquals("bNode", ((TestExecutorSelector)executorSelectorProvider.get()).getSelectorName());
  }

  @Test
  public void testAddRemoveNode() throws Exception {
    // Initial state in the service: a single 'default' node
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("default"));

    // Start populating nodes, make sure we get them back
    serviceSet.testAddNode("one");
    // NB: the TestExecutorSelector adds 'default'. It is *not* part of the TestSet
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "one"));
    serviceSet.testAddNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 3, ImmutableSet.of("default", "one", "two"));
    serviceSet.testRemoveNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "two"));

    // Switching to a new service will get the nodes of the old one
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("secondService");
    // NB: the TestExecutorSelector adds 'secondService'. 'default' was part of the previous TestExecutorSelector,
    // and is it was *not* part of the TestSet. As such, it doesn't get transferred.
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("two", "secondService"));
    serviceSet.testRemoveNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("secondService"));
  }

  /**
   * Test adding and removing nodes, some with compatible version and some with inCompatible version.
   * Verify whether the selectionService excludes inCompatible versions because version check is enabled.
   * @throws Exception
   */
  @Test
  public void testDremioVersionCheckEnabled() throws Exception {
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("default"));

    // Start populating nodes, make sure we get them back
    serviceSet.testAddNode("one");
    // NB: the TestExecutorSelector adds 'default'. It is *not* part of the TestSet
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "one"));
    serviceSet.testAddNode("two", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "one"));
    serviceSet.testRemoveNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("default"));
    serviceSet.testRemoveNode("two", "incompatible");
    serviceSet.testAddNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "two"));

    // Switching to a new service will get the nodes of the old one
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("secondService");
    // NB: the TestExecutorSelector adds 'secondService'. 'default' was part of the previous TestExecutorSelector,
    // and is it was *not* part of the TestSet. As such, it doesn't get transferred.
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("secondService", "two"));
    serviceSet.testRemoveNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("secondService"));
    serviceSet.testAddNode("one", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("secondService"));
    serviceSet.testRemoveNode("one", "incompatible");
    serviceSet.testAddNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("secondService", "one"));
  }

  /**
   * Test adding and removing nodes, some with compatible version and some with inCompatible version.
   * Verify whether the selectionService does NOT excludes inCompatible versions because version check is disabled.
   * @throws Exception
   */
  @Test
  public void testDremioVersionCheckDisabled() throws Exception {
    // Since we are disabling version check (which is enabled by default), we need coordinator restart
    simulateRestartCoordinator(false);

    // Initial state in the service: a single 'default' node
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("default"));

    // Start populating nodes, make sure we get them back
    serviceSet.testAddNode("one");
    // NB: the TestExecutorSelector adds 'default'. It is *not* part of the TestSet
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "one"));
    serviceSet.testAddNode("two", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 3, ImmutableSet.of("default", "one", "two"));
    serviceSet.testRemoveNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "two"));
    serviceSet.testRemoveNode("two", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("default"));
    serviceSet.testAddNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("default", "two"));

    // Switching to a new service will get the nodes of the old one
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE))).thenReturn("secondService");
    // NB: the TestExecutorSelector adds 'secondService'. 'default' was part of the previous TestExecutorSelector,
    // and is it was *not* part of the TestSet. As such, it doesn't get transferred.
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("secondService", "two"));
    serviceSet.testRemoveNode("two");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("secondService"));
    serviceSet.testAddNode("one", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("secondService", "one"));
    serviceSet.testRemoveNode("one", "incompatible");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 1, ImmutableSet.of("secondService"));
    serviceSet.testAddNode("one");
    TestExecutorSelectorUtil.checkExecutors(selectionService, 2, ImmutableSet.of("secondService", "one"));
  }
}
