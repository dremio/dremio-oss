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
import static org.junit.Assert.assertTrue;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** Utilities for the unit tests of ExecutorSelectionService */
public final class TestExecutorSelectorUtil {
  // Utility class. Do not instantiate
  private TestExecutorSelectorUtil() {}

  /** Check whether the state of the selectionService has reached 'expected' */
  private static void checkServiceState(
      ExecutorSelectionService selectionService, Set<String> expectedAddresses) throws Exception {
    checkServiceState(selectionService, expectedAddresses, new ExecutorSelectionContext());
  }

  /** Check whether the state of the selectionService has reached 'expected' */
  private static void checkServiceState(
      ExecutorSelectionService selectionService,
      Set<String> expectedAddresses,
      ExecutorSelectionContext executorSelectionContext)
      throws Exception {
    try (ExecutorSelectionHandle hdl =
        selectionService.getExecutors(expectedAddresses.size(), executorSelectionContext)) {
      Collection<NodeEndpoint> nodes = hdl.getExecutors();
      if (expectedAddresses.size() != nodes.size()) {
        assertEquals(expectedAddresses.size(), nodes.size());
      }
      Set<String> e = new HashSet<>(expectedAddresses);
      for (NodeEndpoint n : nodes) {
        if (!e.contains(n.getAddress())) {
          assertTrue(e.contains(n.getAddress()));
        }
        e.remove(n.getAddress());
      }
      assertTrue(e.isEmpty());
    }
  }

  /** Wait until the executor selection service reaches 'totalNumExecutors' */
  static void waitForExecutors(ExecutorSelectionService selectionService, int totalNumExecutors)
      throws Exception {
    // Processing of events happens in a separate thread. Must give it a moment to act
    ExecutorSelectionServiceImpl service = (ExecutorSelectionServiceImpl) selectionService;
    final int timeoutMs = 5_000; // we expect this to happen a *lot* sooner
    int attemptNum = 0;
    while (++attemptNum < timeoutMs
        && ((service.getNumExecutors() != totalNumExecutors) || !service.eventsCompleted())) {
      try {
        Thread.sleep(1);
      } catch (Exception e) {
      }
    }
    assertEquals(totalNumExecutors, service.getNumExecutors());
  }

  /**
   * Wait until the executor selection service reaches 'totalNumExecutors', then check whether the
   * endpoints returned by the executor selection service match the expected set of endpoints.
   * Please note: only the endpoint's address will be compared
   */
  static void checkExecutors(
      ExecutorSelectionService selectionService,
      int totalNumExecutors,
      Set<String> expectedAddresses)
      throws Exception {
    waitForExecutors(selectionService, totalNumExecutors);
    checkServiceState(selectionService, expectedAddresses);
  }

  /**
   * Wait until the executor selection service reaches 'totalNumExecutors', then check whether the
   * endpoints returned by the executor selection service match the expected set of endpoints. Given
   * executorSelectioinContext is used get the executors from selectionService. Please note: only
   * the endpoint's address will be compared
   */
  static void checkExecutors(
      ExecutorSelectionService selectionService,
      ExecutorSelectionContext executorSelectionContext,
      int totalNumExecutors,
      Set<String> expectedAddresses)
      throws Exception {
    waitForExecutors(selectionService, totalNumExecutors);
    checkServiceState(selectionService, expectedAddresses, executorSelectionContext);
  }
}
