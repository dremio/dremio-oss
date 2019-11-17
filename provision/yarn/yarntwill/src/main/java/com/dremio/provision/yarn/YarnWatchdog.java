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
package com.dremio.provision.yarn;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Watches another process (PID passed in as an argument), and if this process stops responding to
 * requests from this process, kills it.
 * This is intended to be run in a separate process that only runs this class. Complicating this separate
 * process (for example, by running several things in it) increases the chances of the watchdog itself
 * going wrong, and the main process dying as a consequence.
 *
 * When run, the watcher itself spawns two separate threads:
 * - one thread (the PollingLoop) polls the watched process, in an infinite loop
 * - the second thread (the ParentWatcher) watches for EOF on an input stream passed in by the parent process
 * The watcher terminates when either of these two threads terminate (i.e., the parent process is either unhealthy,
 * causing the polling loop thread to exit; or the parent process exited, causing the parent watcher thread to exit)
 */
public class YarnWatchdog {
  private static final Logger logger = Logger.getLogger(YarnWatchdog.class.getName());
  public static final String YARN_WATCHDOG_LOG_LEVEL = "log.level";

  private final PollingLoop pollingLoop;
  private final ParentWatcher parentWatcher;

  /**
   * Initializes a new YarnWatchdog instance.
   * @param watchedPID  watched process PID
   * @param inputStream stream from watched process
   * @param livenessPort  listen socket port number of liveness
   * @param pollTimeoutMs poll timeout, in milliseconds
   * @param pollIntervalMs  poll interval, in milliseconds
   * @param missedPollsBeforeKill missed polls before kill
   * @param maxKillAttempts max attempts to kill the watched process
   * @param killReattemptIntervalMs  kill reattempt interval, in milliseconds
   */
  public YarnWatchdog(final long watchedPID,
                      final InputStream inputStream,
                      final int livenessPort,
                      final int pollTimeoutMs,
                      final int pollIntervalMs,
                      final int missedPollsBeforeKill,
                      final int maxKillAttempts,
                      final int killReattemptIntervalMs) {
    this.pollingLoop = new PollingLoop(new LinuxWatchdogAction(watchedPID, livenessPort, pollTimeoutMs, maxKillAttempts, killReattemptIntervalMs), pollIntervalMs, pollTimeoutMs, missedPollsBeforeKill);
    this.parentWatcher = new ParentWatcher(inputStream);
  }

  public PollingLoop getPollingLoop() {
    return pollingLoop;
  }

  public ParentWatcher getParentWatcher() {
    return parentWatcher;
  }

  /**
   * Poll & Kill actions performed by the watchdog
   */
  public interface WatchdogAction extends AutoCloseable {
    /**
     * Poll the watched process
     * @return true, if the poll succeeded; false if it failed
     */
    boolean doPoll();

    /**
     * Kill the watched process
     */
    void doKill();
  }

  /**
   * Embodies the polling loop. Its {@link #doWatch()} method will keep polling the watched process until either the
   * polling loop is interrupted with {@link #stopWatching()}, or until the watched process fails to respond a given
   * number of times
   */
  static class PollingLoop {
    private final WatchdogAction watchdogAction;
    private final int pollIntervalMs;
    private final int pollTimeoutMs;
    private final int missedPollsBeforeKill;

    // State:
    private volatile boolean running;
    private int numFailedPolls;

    /**
     * @param pollIntervalMs         Time, in milliseconds, between two consecutive polls
     * @param pollTimeoutMs          Time, in milliseconds, to wait for reply
     * @param missedPollsBeforeKill  Number of missed polls before the watched process is killed
     */
    PollingLoop(final WatchdogAction watchdogAction, final int pollIntervalMs, final int pollTimeoutMs, final int missedPollsBeforeKill) {
      this.watchdogAction = watchdogAction;
      this.pollIntervalMs = pollIntervalMs;
      this.pollTimeoutMs = pollTimeoutMs;
      this.missedPollsBeforeKill = missedPollsBeforeKill;

      this.running = true;
      this.numFailedPolls = 0;
    }

    public boolean isRunning() {
      return running;
    }

    /**
     * The main watchdog loop. Keeps polling the watched process until either:
     * - the watched process stops responding, at which time the watched process is killed; or
     * - the loop is interrupted by a call to {@link #stopWatching()}
     */
    public void doWatch() {
      logger.log(Level.INFO,"Started watchdog");

      long lastPollSucceedTime = System.currentTimeMillis();
      while (running) {
        if (watchdogAction.doPoll()) {
          logger.log(Level.FINE,"Watchdog poll succeed.");
          numFailedPolls = 0;
          lastPollSucceedTime = System.currentTimeMillis();
        } else {
          ++numFailedPolls;
          if (numFailedPolls < missedPollsBeforeKill) {
            logger.log(Level.INFO,"Watchdog poll failed. Number failed polls currently at {0}", numFailedPolls);
            long elapsedTime = System.currentTimeMillis() - lastPollSucceedTime;
            if (elapsedTime > (pollIntervalMs + pollTimeoutMs) * missedPollsBeforeKill) {
              /* Watchdog is expected to complete missedPollsBeforeKill polls, but actually it's not, because it might be
                 slow down for the whole system is unhealthy. We should kill watched process, otherwise watchdog might not
                 be able to get a chance to do next poll if the system become totally unhealthy.
               */
              logger.log(Level.SEVERE, "Watchdog is unhealthy, elapsedTime is %dms, numFailedPolls is {0}, pollIntervalMs is {1}, pollTimeoutMs is {2}, missedPollsBeforeKill is {3}. Issuing process kill",
                new Object[] {elapsedTime, numFailedPolls, pollIntervalMs, pollTimeoutMs, missedPollsBeforeKill});
              watchdogAction.doKill();
              running = false;
              break;
            }
          } else {
            logger.log(Level.SEVERE,"Watchdog detected {0} failed polls. Issuing process kill", numFailedPolls);
            watchdogAction.doKill();
            running = false;
            break;
          }
        }

        // Implementation note: An alternative to blindly sleeping 'pollIntervalMs' would have been to keep 'pollIntervalMs'
        // milliseconds between the starts of consecutive polls. However, we expect the polling interval to be
        // significantly larger than the timeout inside the poll itself (see LinuxWatchdogAction, below), therefore
        // rendering this optimization pointless
        try {
          Thread.sleep(pollIntervalMs);
        } catch (InterruptedException e) {
          // ignore the exception
        }
        // All we need to do here is loop around and check for the 'running' flag
      }
      try {
        watchdogAction.close();
      } catch (Exception e) {
        logger.log(Level.SEVERE,"Closing the watchdog failed {0}", exceptionStacktraceToString(e));
      }
      logger.log(Level.INFO,"Watchdog exiting normally");
    }

    /**
     * Stops the (otherwise infinite) watchdog loop
     */
    public void stopWatching() {
      running = false;
    }
  }

  /**
   * Thread to run the polling loop.
   */
  static class PollingLoopThread extends Thread {

    private final PollingLoop pollingLoop;

    public PollingLoopThread(final PollingLoop pollingLoop) {
      this.pollingLoop = pollingLoop;
    }

    @Override
    public void run() {
      pollingLoop.doWatch();
      logger.log(Level.INFO,"Watchdog thread exited");
    }
  }

  /**
   * Simply reads from the input stream and discards its inputs, until the input stream closes
   */
  static class ParentWatcher {
    private final InputStream inputStream;
    private volatile boolean running;

    ParentWatcher(final InputStream inputStream) {
      this.inputStream = inputStream;
      this.running = true;
    }

    public boolean isRunning() {
      return running;
    }

    /**
     * Watch for an EOF on the input stream
     */
    void watchInput() {
      try {
        // The only message ever received on STDIN will be an EOF. Consuming input just in case
        int val;
        while ((val = inputStream.read()) != -1) {
          // NB: no-op, but checkstyle requires at least one statement. Hence, debug message
          logger.log(Level.FINE,"Received %d at input", val);
        }
        logger.log(Level.INFO,"EOF on watchdog input. Quitting");
      } catch (IOException e) {
        logger.log(Level.INFO,"I/O exception on watchdog input. Quitting %s", exceptionStacktraceToString(e));
      } finally {
        running = false;
      }
    }
  }

  /**
   * Default actions, on Linux. Uses system-specific commands used to kill the other process
   */
  static class LinuxWatchdogAction implements WatchdogAction {

    private final long watchedPid;
    private final String urlToRead;
    private final int pollTimeoutMs;
    private final int maxKillTimes;
    private final int killReattemptIntervalMs;

    public LinuxWatchdogAction(final long watchedPid,
                               final int livenessPort,
                               final int pollTimeoutMs,
                               final int maxKillTimes,
                               final int killReattemptIntervalMs) {
      this.watchedPid = watchedPid;
      this.urlToRead = String.format("http://localhost:%d/live", livenessPort);
      this.pollTimeoutMs = pollTimeoutMs;
      this.maxKillTimes = maxKillTimes;
      this.killReattemptIntervalMs = killReattemptIntervalMs;
    }

    @Override
    public boolean doPoll() {
      final StringBuilder result = new StringBuilder();
      try {
        final URL url = new URL(urlToRead);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setReadTimeout(pollTimeoutMs);
        final BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
          result.append(line);
        }
        rd.close();
      } catch (Exception e) {
        return false;
      }
      return true;
    }

    @Override
    public void doKill() {
      int retryCount = 0;
      while (retryCount < maxKillTimes) {
        try {
          if (retryCount < (maxKillTimes / 2)) {
            // Try to kill watched process gracefully
            Runtime.getRuntime().exec("kill " + watchedPid);
          } else {
            // Try to kill watched process forcefully
            Runtime.getRuntime().exec("kill -9 " + watchedPid);
          }
          if (!isProcessRunning(watchedPid)) {
            return;
          }
        } catch (IOException e) {
          logger.log(Level.WARNING, "Failed to kill parent process (pid: %d) for attempt %d.", new Object[]{watchedPid, retryCount});
        }
        try {
          Thread.sleep(killReattemptIntervalMs);
        } catch (InterruptedException e){
          // ignore the exception
        }
        retryCount++;
      }
    }

    private boolean isProcessRunning(final long pid) {
      final String command = "ps -p " + pid;
      logger.log(Level.FINE,"Check process command [%s]", command);
      try {
        final Process process = Runtime.getRuntime().exec(command);
        final InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
        final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String strLine;
        while ((strLine = bufferedReader.readLine()) != null) {
          if (strLine.contains(" " + pid + " ") || strLine.startsWith(pid + " ")) {
            logger.log(Level.FINE,"Process %d is still running.", pid);
            return true;
          }
        }
        logger.log(Level.FINE,"Process %d is not running.", pid);
        return false;
      } catch (Exception e) {
        logger.log(Level.WARNING, "Got exception using system command [%s]. %s", new Object[]{command, exceptionStacktraceToString(e)});
        return true;
      }
    }

    @Override
    public void close() {

    }
  }

  private static void dumpUsage(String errorMessage) {
    logger.log(Level.SEVERE, errorMessage);
    logger.log(Level.SEVERE, "Usage: YarnWatchdog <watchedPID> <livenessPort> <pollTimeoutMs> <pollIntervalMs> <missedPollsBeforeKill> <maxKillAttempts> <killReattemptIntervalMs>");
  }


  /**
   * Main entry point of the watchdog process
   *   - the program instantaneously quit when it receives an EOF on System.in
   *   - the program polls the watched process. If a number of polls fail, the program kills the watched process
   * Arguments:
   *   [0] - watched process PID
   *   [1] - listen socket port number of liveness
   *   [2] - poll timeout, in milliseconds
   *   [3] - poll interval, in milliseconds
   *   [4] - missed polls before kill
   *   [5] - max attempts to kill the watched process
   *   [6] - kill reattempt interval, in milliseconds
   */
  public static void main(final String[] args) throws Exception {
    setLogLevel();
    logger.log(Level.INFO, "YarnWatchdog invoked: %s", String.join(", ", args));
    if (args.length != 7) {
      dumpUsage("Incorrect number of arguments");
      System.exit(1);
    }
    long watchedPID = 0;
    int livenessPort = 0;
    int pollTimeoutMs = 0;
    int pollIntervalMs = 0;
    int missedPollsBeforeKill = 0;
    int maxKillAttempts = 0;
    int killReattemptIntervalMs = 0;

    try {
      watchedPID =  Long.parseLong(args[0]);
      livenessPort = Integer.parseInt(args[1]);
      pollTimeoutMs = Integer.parseInt(args[2]);
      pollIntervalMs = Integer.parseInt(args[3]);
      missedPollsBeforeKill = Integer.parseInt(args[4]);
      maxKillAttempts = Integer.parseInt(args[5]);
      killReattemptIntervalMs = Integer.parseInt(args[6]);
    } catch (NumberFormatException e) {
      dumpUsage("Incorrectly formatted argument");
      System.exit(3);
    }

    logger.log(Level.INFO, "YarnWatchdog, watchedPID=%d, livenessPort=%d, pollTimeoutMs=%d, pollIntervalMs=%d, missedPollsBeforeKill=%d, maxKillAttempts=%d, killReattemptIntervalMs=%d",
      new Object[]{watchedPID, livenessPort, pollTimeoutMs, pollIntervalMs, missedPollsBeforeKill, maxKillAttempts, killReattemptIntervalMs});

    YarnWatchdog yarnWatchdog = new YarnWatchdog(watchedPID, System.in, livenessPort, pollTimeoutMs, pollIntervalMs, missedPollsBeforeKill, maxKillAttempts, killReattemptIntervalMs);
    // start polling loop thread
    final PollingLoopThread pollingLoopThread = new PollingLoopThread(yarnWatchdog.getPollingLoop());
    pollingLoopThread.start();

    // start parent watcher thread
    final Thread parentWatcherThread = new Thread() {
      @Override
      public void run() {
        yarnWatchdog.getParentWatcher().watchInput();
        System.exit(0);
      }
    };
    parentWatcherThread.start();

    // wait until polling loop thread exit
    pollingLoopThread.join();
    System.exit(0);
  }

  public static String exceptionStacktraceToString(Exception e)
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayOutputStream);
    e.printStackTrace(printStream);
    printStream.close();
    return byteArrayOutputStream.toString();
  }

  private static class YarnWatchdogFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
      return String.format("%1$tF %1$tT,%1$tL [%2$s] %3$-7s %4$s - %5$s %n", new Object[]{new Date(record.getMillis()),
        Thread.currentThread().getName(), record.getLevel(), record.getSourceClassName(),
        String.format(record.getMessage(), record.getParameters())});
    }
  }

  private static void setLogLevel() {
    final String levelString = System.getProperty(YARN_WATCHDOG_LOG_LEVEL, "");
    Level level;
    switch (levelString) {
      case "ERROR":
        level = Level.SEVERE;
        break;
      case "WARN":
        level = Level.WARNING;
        break;
      case "INFO":
        level = Level.INFO;
        break;
      case "DEBUG":
        level = Level.FINE;
        break;
      case "TRACE":
        level = Level.FINEST;
        break;
      default:
        logger.log(Level.WARNING, "log level [{0}] is unrecognized", levelString);
        level = Level.INFO;
        break;
    }
    final Logger root = Logger.getLogger("");
    final YarnWatchdogFormatter yarnWatchdogFormatter = new YarnWatchdogFormatter();
    root.setLevel(level);
    for (Handler handler : root.getHandlers()) {
      handler.setLevel(level);
      handler.setFormatter(yarnWatchdogFormatter);
    }
  }
}
