/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.provision.yarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.Field;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.Test;

/**
 * Tests for YarnWatchdog
 */
public class TestYarnWatchdog {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestYarnWatchdog.class);

  /**
   * Responds with a series of pre-computed poll responses. Tracks when it was supposed to be killed
   */
  static class TestWatchdogAction implements YarnWatchdog.WatchdogAction {
    // Inputs:
    private final int[] pollResponses;        // responses sent to every poll. Polls beyond the last precomputed poll will return success
    private final int missedPollsBeforeKill;  // How many missed polls before watchdog kills us. Used to check for successful completion
    private final int killTick;               // Tick at which we were expected to issue a kill
    // State:
    private int currentTick;    // Index into 'pollResponses'
    private boolean killed;

    TestWatchdogAction(int[] pollResponses, int missedPollsBeforeKill, int killTick) {
      this.pollResponses = pollResponses;
      this.missedPollsBeforeKill = missedPollsBeforeKill;
      this.killTick = killTick;
      this.currentTick = 0;
      this.killed = false;
    }

    @Override
    public boolean doPoll() {
      boolean result = (currentTick >= pollResponses.length) || (pollResponses[currentTick] == 1);
      logger.info("Tick: {}, Result: {}, killTick: {}", currentTick, result, killTick);
      if (result) {
        assertTrue(killTick == -1 || currentTick < killTick);
      } else {
        assertTrue(killTick == -1 || currentTick <= killTick);  // NB: we might be testing missing signals
      }
      ++currentTick;
      return result;
    }

    @Override
    public void doKill() {
      logger.info("Kill. Tick: {}, killTick: {}", currentTick, killTick);
      assertNotEquals(-1, killTick);
      assertEquals(killTick + 1, currentTick);
      killed = true;
    }

    @Override
    public void close() {
      assertTrue(killTick == -1 || currentTick == killTick);
    }

    boolean isDone() {
      return killed || (currentTick >= pollResponses.length);
    }

    int getMissedPollsBeforeKill() {
      return missedPollsBeforeKill;
    }

    int getExpectedTicks() {
      return pollResponses.length;
    }
  }

  /**
   * Creates a watchdog, running in a separate thread. Runs until the action runs out of responses, or
   * alternatively, until it fails.
   */
  void doPollingTest(TestWatchdogAction action) throws Exception {
    final YarnWatchdog.PollingLoop pollingLoop = new YarnWatchdog.PollingLoop(action, 1, 10_000, action.getMissedPollsBeforeKill());
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        pollingLoop.doWatch();
        logger.info("Watchdog thread exited");
      }
    });
    t.start();
    int count = 0;
    final int tooLong = action.getExpectedTicks() + 1_000; // 1s buffer
    while (!action.isDone()) {
      Thread.sleep(1);
      ++count;
      assertTrue(count < tooLong);
    }
    pollingLoop.stopWatching();
    t.join();
  }

  // Sunny-day test: poll successful, until explicit interruption
  @Test
  public void testPollSuccess() throws Exception {
    doPollingTest(new TestWatchdogAction(new int[] {1, 1, 1, 1, 1, 1}, 1, -1));
  }

  // Watched process stops responding. Kill after N retries
  @Test
  public void testPollKillAfterN() throws Exception {
    doPollingTest(new TestWatchdogAction(new int[] {1, 1, 0, 0, 1, 1}, 2, 3));
  }

  // Watched process recovers after a few missed tries. Success afterwards
  @Test
  public void testPollRecovery() throws Exception {
    doPollingTest(new TestWatchdogAction(new int[] {1, 1, 0, 0, 1, 0, 1}, 3, -1));
  }

  // Parent watcher thread runs until EOF on input
  @Test
  public void testParentWatcher() throws Exception {
    final PipedOutputStream pOut = new PipedOutputStream();
    final PipedInputStream pIn = new PipedInputStream(pOut);
    final YarnWatchdog.ParentWatcher pw = new YarnWatchdog.ParentWatcher(pIn);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        pw.watchInput();
        logger.info("Parent watcher thread exited");
      }
    });
    t.start();

    t.join(1);
    assertTrue(t.isAlive());

    pOut.write(15);
    t.join(1);
    assertTrue(t.isAlive());

    pOut.close();
    t.join();
    assertFalse(t.isAlive());
  }

  // "process" tests: test the integration of the two watchdog threads: the watchdog loop and the parent watcher

  /**
   * Servlet used to respond to liveness requests. Simply returns a 'yes, I'm alive'
   */
  public static class TestLivenessServlet extends HttpServlet
  {
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response ) throws ServletException, IOException
    {
      response.setStatus(HttpServletResponse.SC_OK);
    }
  }

  /**
   * In Java9 there's native support for p.pid(). Until then, a workaround.
   * WARNING: will throw on windows
   */
  long getPid(Process p) throws Exception {
    long pid = -1;

    if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
      Field f = p.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      pid = f.getLong(p);
      f.setAccessible(false);
    }
    return pid;
  }

  enum ProcessTestMode {
    HEALTHY,
    FAIL_HEALTH,
    FAIL_PROCESS
  };

  /**
   * Wait for parent watcher to end, will exit after 30 seconds.
   * @param parentWatcher
   * @throws Exception
   */
  private void waitForEndWithTimeout(final YarnWatchdog.ParentWatcher parentWatcher) throws Exception {
    int numAttempts = 0;
    while (parentWatcher.isRunning() && numAttempts < 30_000) {
      Thread.sleep(1);
      ++numAttempts;
    }
  }

  /**
   * Wait for polling loop to end, will exit after 30 seconds.
   * @param pollingLoop
   * @throws Exception
   */
  private void waitForEndWithTimeout(final YarnWatchdog.PollingLoop pollingLoop) throws Exception {
    int numAttempts = 0;
    while (pollingLoop.isRunning() && numAttempts < 30_000) {
      Thread.sleep(1);
      ++numAttempts;
    }
  }

  /**
   * Starts an HTTP server, a side process (using 'cat', since we need something that doesn't actually write anything)
   * and then invokes the watchdog's main.
   *
   * The test itself runs the http server (normally part of Dremio), as well as the watchdog (normally part of the
   * YarnWatchdog). The watchdog needs a process that it watches for failures -- we spawn a process for that purpose.
   *
   * We expect the watchdog's main to keep running until we explicitly make things fail (either by failing the health
   * check, or by killing the side process)
   *
   * WARNING: only runs on Mac & Linux.
   */
  private void doProcessTest(final ProcessTestMode processTestMode, final long millis) throws Exception {
    final String loopbackInterface = "127.0.0.1";
    final Server embeddedServer = new Server();
    final ServerConnector serverConnector = new ServerConnector(embeddedServer);
    serverConnector.setPort(0);  // autodetect
    serverConnector.setHost(loopbackInterface);
    embeddedServer.addConnector(serverConnector);
    ServletHandler handler = new ServletHandler();
    embeddedServer.setHandler(handler);
    handler.addServletWithMapping(TestLivenessServlet.class, "/live");
    embeddedServer.start();
    final int livenessPort = serverConnector.getLocalPort();

    final ProcessBuilder pb = new ProcessBuilder("cat");
    Process sideProcess = pb.start();
    InputStream sideProcessOutput = sideProcess.getInputStream();  // output of the side proces (== side process' inputStream)

    final YarnWatchdog yarnWatchdog = new YarnWatchdog(getPid(sideProcess), sideProcessOutput, livenessPort, 10_000, 1, 2, 10, 1);
    final YarnWatchdog.PollingLoop pollingLoop = yarnWatchdog.getPollingLoop();
    final YarnWatchdog.ParentWatcher parentWatcher = yarnWatchdog.getParentWatcher();

    // start polling loop thread
    final YarnWatchdog.PollingLoopThread pollingLoopThread = new YarnWatchdog.PollingLoopThread(pollingLoop);
    pollingLoopThread.start();

    final Thread parentWatcherThread = new Thread() {
      @Override
      public void run() {
        parentWatcher.watchInput();
      }
    };
    parentWatcherThread.start();

    Thread.sleep(millis);

    switch (processTestMode) {
      case HEALTHY:
        assertTrue(pollingLoop.isRunning());
        assertTrue(parentWatcher.isRunning());
        embeddedServer.stop();
        break;
      case FAIL_HEALTH:
        embeddedServer.stop();
        waitForEndWithTimeout(parentWatcher);
        assertFalse(parentWatcher.isRunning());
        waitForEndWithTimeout(pollingLoop);
        assertFalse(pollingLoop.isRunning());
        break;
      case FAIL_PROCESS:
        sideProcess.destroy();
        waitForEndWithTimeout(parentWatcher);
        assertFalse(parentWatcher.isRunning());
        assertTrue(pollingLoop.isRunning());
        pollingLoop.stopWatching();
        break;
      default:
        throw new IllegalStateException("Invalid process test mode " + processTestMode);
    }
    parentWatcherThread.join();
    pollingLoopThread.join();
  }

  // Test parent process health check succeed
  @Test
  public void testProcessHealthy() throws Exception {
    // succeed after 300ms
    doProcessTest(ProcessTestMode.HEALTHY, 300);
  }

  // Test parent process health check failure
  @Test
  public void testProcessUnhealthy() throws Exception {
    // stop liveness server after 300ms
    doProcessTest(ProcessTestMode.FAIL_HEALTH, 300);
  }

  // Test parent process failure
  @Test
  public void testProcessDeath() throws Exception {
    // kill 'cat' process after 300ms
    doProcessTest(ProcessTestMode.FAIL_PROCESS, 300);
  }
}
