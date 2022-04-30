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
package com.dremio.exec.testing;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.slf4j.Logger;

import com.dremio.BaseTestQuery;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ZookeeperHelper;
import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;

public class TestPauseInjection extends BaseTestQuery {

  private static final UserSession session = UserSession.Builder.newBuilder()
    .withSessionOptionManager(
      new SessionOptionManagerImpl(nodes[0].getContext().getOptionValidatorListing()),
      nodes[0].getContext().getOptionManager())
    .withCredentials(UserCredentials.newBuilder()
        .setUserName(UserServiceTestImpl.TEST_USER_1)
        .build())
      .withUserProperties(UserProperties.getDefaultInstance())
      .build();

  /**
   * Class whose methods we want to simulate pauses at run-time for testing
   * purposes. The class must have access to {@link com.dremio.exec.ops.QueryContext} or
   * {@link com.dremio.sabot.exec.fragment.FragmentContext}.
   */
  private static class DummyClass {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(DummyClass.class);
    private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(DummyClass.class);

    private final QueryContext context;
    private final CountDownLatch latch;

    public DummyClass(final QueryContext context, final CountDownLatch latch) {
      this.context = context;
      this.latch = latch;
    }

    public static final String PAUSES = "<<pauses>>";

    /**
     * Method that pauses.
     *
     * @return how long the method paused in milliseconds
     */
    public long pauses() {
      // ... code ...

      latch.countDown();
      final long startTime = System.currentTimeMillis();
      // simulated pause
      injector.injectPause(context.getExecutionControls(), PAUSES, logger);
      final long endTime = System.currentTimeMillis();

      // ... code ...
      return (endTime - startTime);
    }
  }

  private static class ResumingThread extends Thread {

    private final QueryContext context;
    private final ExtendedLatch latch;
    private final Pointer<Exception> ex;
    private final long millis;

    public ResumingThread(final QueryContext context, final ExtendedLatch latch, final Pointer<Exception> ex,
                          final long millis) {
      this.context = context;
      this.latch = latch;
      this.ex = ex;
      this.millis = millis;
    }

    @Override
    public void run() {
      latch.awaitUninterruptibly();
      try {
        Thread.sleep(millis);
      } catch (final InterruptedException ex) {
        this.ex.value = ex;
      }
      context.getExecutionControls().unpauseAll();
    }
  }

  @Test
  public void pauseInjected() {
    final long expectedDuration = 1000L;
    final ExtendedLatch trigger = new ExtendedLatch(1);
    final Pointer<Exception> ex = new Pointer<>();

    final String controls = Controls.newBuilder()
      .addPause(DummyClass.class, DummyClass.PAUSES)
      .build();

    ControlsInjectionUtil.setControls(session, controls);

    final QueryContext queryContext = new QueryContext(session, nodes[0].getContext(), QueryId.getDefaultInstance());

    (new ResumingThread(queryContext, trigger, ex, expectedDuration)).start();

    // test that the pause happens
    final DummyClass dummyClass = new DummyClass(queryContext, trigger);
    final long actualDuration = dummyClass.pauses();
    assertTrue(String.format("Test should stop for at least %d milliseconds.", expectedDuration),
      expectedDuration <= actualDuration);
    assertTrue("No exception should be thrown.", ex.value == null);
    try {
      queryContext.close();
    } catch (final Exception e) {
      fail("Failed to close query context: " + e);
    }
  }

  @Test
  public void pauseOnSpecificBit() throws Exception {
    final ZookeeperHelper zkHelper = new ZookeeperHelper();
    zkHelper.startZookeeper(1);

    final SabotConfig config = zkHelper.getConfig();
    final ScanResult classpathScanResult = ClassPathScanner.fromPrescan(config);
    // Creating two nodes
    try (
      ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
      SabotNode node1 = SabotNode.start(config, clusterCoordinator, classpathScanResult);
      SabotNode node2 = SabotNode.start(config, clusterCoordinator, classpathScanResult)) {
      final SabotContext nodeContext1 = node1.getContext();
      final SabotContext nodeContext2 = node2.getContext();

      final UserSession session = UserSession.Builder.newBuilder()
        .withSessionOptionManager(
          new SessionOptionManagerImpl(nodeContext1.getOptionValidatorListing()),
          nodeContext1.getOptionManager())
        .withCredentials(UserCredentials.newBuilder()
          .setUserName(UserServiceTestImpl.TEST_USER_1)
          .build())
        .withUserProperties(UserProperties.getDefaultInstance())
        .build();

      final NodeEndpoint nodeEndpoint1 = nodeContext1.getEndpoint();
      final String controls = Controls.newBuilder()
        .addPauseOnNode(DummyClass.class, DummyClass.PAUSES, nodeEndpoint1)
        .build();

      ControlsInjectionUtil.setControls(session, controls);

      {
        final long expectedDuration = 1000L;
        final ExtendedLatch trigger = new ExtendedLatch(1);
        final Pointer<Exception> ex = new Pointer<>();
        final QueryContext queryContext = new QueryContext(session, nodeContext1, QueryId.getDefaultInstance());
        (new ResumingThread(queryContext, trigger, ex, expectedDuration)).start();

        // test that the pause happens
        final DummyClass dummyClass = new DummyClass(queryContext, trigger);
        final long actualDuration = dummyClass.pauses();
        assertTrue(String.format("Test should stop for at least %d milliseconds.", expectedDuration),
          expectedDuration <= actualDuration);
        assertTrue("No exception should be thrown.", ex.value == null);
        try {
          queryContext.close();
        } catch (final Exception e) {
          fail("Failed to close query context: " + e);
        }
      }

      {
        final ExtendedLatch trigger = new ExtendedLatch(1);
        final QueryContext queryContext = new QueryContext(session, nodeContext2, QueryId.getDefaultInstance());

        // if the resume did not happen, the test would hang
        final DummyClass dummyClass = new DummyClass(queryContext, trigger);
        dummyClass.pauses();
        try {
          queryContext.close();
        } catch (final Exception e) {
          fail("Failed to close query context: " + e);
        }
      }
    } catch (final NodeStartupException e) {
      throw new RuntimeException("Failed to start two nodes.", e);
    }
  }
}
