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
package com.dremio.service.commandpool;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.util.Closeable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

import io.opentracing.noop.NoopTracerFactory;

/**
 * Tests for {@link ReleasableBoundCommandPool}
 */
public class TestReleasableBoundCommandPool extends TestBoundCommandPool {
  private final AtomicInteger counter = new AtomicInteger(0);

  @Override
  CommandPool newTestCommandPool() {
    return createReleasableCommandPool(1);
  }

  ReleasableBoundCommandPool createReleasableCommandPool(int size) {
    return new ReleasableBoundCommandPool(size, NoopTracerFactory.create());
  }

  // This tests for the following:
  // A max of poolSize threads are scheduled at any point of time. The remaining are queued
  // The set of threadIds tracked by the ReleasableBoundCommandPool tracks the threadIds correctly
  @Test
  public void testPoolSizeAndThreadIds() throws Exception {
    // create a command pool of size 2
    ReleasableBoundCommandPool commandPool = createReleasableCommandPool(2);

    // create block commands that can be unblocked one by one
    List<CommandPool.Command<?>> allCommands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread1Commands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread2Commands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread3Commands = Lists.newArrayList();

    BlockingCommand command;
    CountMaxAndCheckThreadIds countMax = new CountMaxAndCheckThreadIds(commandPool);
    for(int i = 0; i < 10; i++) {
      command = new BlockingCommand(countMax);
      allCommands.add(command);
      thread1Commands.add(command);

      command = new BlockingCommand(countMax);
      allCommands.add(command);
      thread2Commands.add(command);

      command = new BlockingCommand(countMax);
      allCommands.add(command);
      thread3Commands.add(command);
    }

    TaskSubmissionThread taskSubmissionThread1 = new TaskSubmissionThread(thread1Commands, commandPool, 1);
    TaskSubmissionThread taskSubmissionThread2 = new TaskSubmissionThread(thread2Commands, commandPool, 2);
    TaskSubmissionThread taskSubmissionThread3 = new TaskSubmissionThread(thread3Commands, commandPool, 3);

    // start all 3 threads
    taskSubmissionThread1.start();
    taskSubmissionThread2.start();
    taskSubmissionThread3.start();

    for(CommandPool.Command<?> task : allCommands) {
      BlockingCommand blockingCommand = (BlockingCommand)task;
      blockingCommand.unblock();
      Assert.assertTrue(countMax.maxCount <= 2);
      try {
        Thread.sleep(2);
      } catch (InterruptedException ie) {
      }
    }

    taskSubmissionThread1.join();
    taskSubmissionThread2.join();
    taskSubmissionThread3.join();
  }

  // This test tests releaseAndReacquireSlot API. The thread holding the slot releases and waits for a event
  @Test
  public void testReleaseAndWait() {
    ReleasableBoundCommandPool releasableBoundCommandPool = createReleasableCommandPool(1);

    ReleaseAndWaitCommand releaseAndWaitCommand = new ReleaseAndWaitCommand(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<?> future = releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "release-and-block", "", releaseAndWaitCommand, false);
    releaseAndWaitCommand.firstUnblock();
    releaseAndWaitCommand.secondUnblock();
    try {
      future.get();
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      throw new RuntimeException(e);
    }
  }

  // This test tests releaseAndReacquireSlot API. The thread holding the slot releases and submits a task to
  // the command pool and waits for it to complete
  @Test
  public void testReleaseAndSubmitTask() {
    ReleasableBoundCommandPool releasableBoundCommandPool = createReleasableCommandPool(1);

    ReleaseAndResubmitTask releaseAndResubmitTask = new ReleaseAndResubmitTask(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<?> future = releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "release-and-submit", "", releaseAndResubmitTask, false);
    releaseAndResubmitTask.firstUnblock();
    releaseAndResubmitTask.secondUnblock();
    try {
      future.get();
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      throw new RuntimeException(e);
    }
  }

  // test to ensure that the reacquiring waiter has higher priority than a regular task
  @Test
  public void testOrderingOfReAcquiringWaiters() throws Exception {
    ReleasableBoundCommandPool releasableBoundCommandPool = createReleasableCommandPool(1);

    counter.set(0);
    ReleaseAndResubmitTask releaseAndResubmitTask = new ReleaseAndResubmitTask(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<Integer> future = releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "release-and-submit", "", releaseAndResubmitTask, false);

    Thread.sleep(2);

    SimpleBlockingCommand blockingCommand1 = new SimpleBlockingCommand(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<Integer> future1 = releasableBoundCommandPool.submit(CommandPool.Priority.MEDIUM, "simple1", "", blockingCommand1, false);

    Thread.sleep(2);

    SimpleBlockingCommand blockingCommand2 = new SimpleBlockingCommand(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<Integer> future2 = releasableBoundCommandPool.submit(CommandPool.Priority.MEDIUM, "simple2", "", blockingCommand2, false);

    Thread.sleep(2);

    SimpleBlockingCommand blockingCommand3 = new SimpleBlockingCommand(releasableBoundCommandPool, new StartAndStop());
    CompletableFuture<Integer> future3 = releasableBoundCommandPool.submit(CommandPool.Priority.MEDIUM, "simple3", "", blockingCommand3, false);

    // will release the slot and submit a job
    // blockingCommand1 should run now
    releaseAndResubmitTask.firstUnblock();

    // wait till the new job submission is done
    Awaitility.await()
      .pollInterval(Duration.ofSeconds(1))
      .atMost(Duration.ofSeconds(50))
      .until(() -> releasableBoundCommandPool.getNumWaiters() == 3);

    // simpleBlockingCommand1 is done
    // 3 waiters: blockingCommand2, blockingCommand3 (MEDIUM priority) and the job submitted by releaseAndResubmitTask (HIGH priority)
    // after this unblock submitted task should run
    blockingCommand1.firstUnblock();

    // 2 waiters now: blockingCommand2 and blockingCommand3. blockingCommand2 should run
    // releaseAndResubmitTask is still not a waiter since close is not invoked
    releaseAndResubmitTask.secondUnblock();

    // ensure that releaseAndResubmitTask is in the waiter queue
    Awaitility.await()
      .pollInterval(Duration.ofSeconds(1))
      .atMost(Duration.ofSeconds(50))
      .until(() -> releasableBoundCommandPool.getReacquireWaiters() == 1);

    // this should give control to releaseAndResubmitTask
    blockingCommand2.firstUnblock();

    blockingCommand3.firstUnblock();

    Assert.assertEquals(1, (int)Futures.getUnchecked(future1));
    Assert.assertEquals(2, (int)Futures.getUnchecked(future2));
    Assert.assertEquals(3, (int)Futures.getUnchecked(future));
    Assert.assertEquals(4, (int)Futures.getUnchecked(future3));
  }

  List<ReleasingCommand> create3RandomTasks(int i, ReleasableBoundCommandPool commandPool, StartAndStop startAndStop) {
    ReleasingCommand cmd1;
    ReleasingCommand cmd2;
    ReleasingCommand cmd3;

    int reminder = (i % 3);
    if (reminder == 0) {
      cmd1 = new SimpleBlockingCommand(commandPool, startAndStop);
      cmd2 = new ReleaseAndResubmitTask(commandPool, startAndStop);
      cmd3 = new ReleaseAndWaitCommand(commandPool, startAndStop);
    } else if (reminder == 1) {
      cmd2 = new SimpleBlockingCommand(commandPool, startAndStop);
      cmd3 = new ReleaseAndResubmitTask(commandPool, startAndStop);
      cmd1 = new ReleaseAndWaitCommand(commandPool, startAndStop);
    } else {
      cmd3 = new SimpleBlockingCommand(commandPool, startAndStop);
      cmd1 = new ReleaseAndResubmitTask(commandPool, startAndStop);
      cmd2 = new ReleaseAndWaitCommand(commandPool, startAndStop);
    }

    return Lists.newArrayList(cmd1, cmd2, cmd3);
  }

  // test to ensure that when the API releaseAndReacquireSlot API is used, the number of active threads does not increase
  @Test
  public void testActiveThreads() throws Exception {
    ReleasableBoundCommandPool commandPool = createReleasableCommandPool(2);

    // create command that release and reacquire the slot
    List<CommandPool.Command<?>> allCommands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread1Commands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread2Commands = Lists.newArrayList();
    List<CommandPool.Command<?>> thread3Commands = Lists.newArrayList();

    CountMaxAndCheckThreadIds countMaxAndCheckThreadIds = new CountMaxAndCheckThreadIds(commandPool);
    for(int i = 0; i < 10; i++) {
      List<ReleasingCommand> tasks = create3RandomTasks(i, commandPool, countMaxAndCheckThreadIds);

      allCommands.addAll(tasks);
      thread1Commands.add(tasks.get(0));
      thread2Commands.add(tasks.get(1));
      thread3Commands.add(tasks.get(2));
    }

    TaskSubmissionThread thread1 = new TaskSubmissionThread(thread1Commands, commandPool, 1);
    TaskSubmissionThread thread2 = new TaskSubmissionThread(thread2Commands, commandPool, 2);
    TaskSubmissionThread thread3 = new TaskSubmissionThread(thread3Commands, commandPool, 3);

    // start all threads
    thread1.start();
    thread2.start();
    thread3.start();

    ReleasingCommand prevCmd = null;
    for(CommandPool.Command<?> cmd : allCommands) {
      ReleasingCommand currCmd = (ReleasingCommand) cmd;

      currCmd.firstUnblock();
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
      }

      if (prevCmd != null) {
        prevCmd.secondUnblock();
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
        }
      }

      prevCmd = currCmd;
      Assert.assertTrue(countMaxAndCheckThreadIds.maxCount <= 2);
    }

    prevCmd.secondUnblock();
    thread1.join();
    thread2.join();
    thread3.join();
  }

  // test jobs submitted using the same thread
  @Test
  public void testSubmitInSameThread() throws Exception {
    ReleasableBoundCommandPool releasableBoundCommandPool = createReleasableCommandPool(1);
    SubmitInSameThread submitInSameThread = new SubmitInSameThread(releasableBoundCommandPool);

    CompletableFuture<?> future = releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "submit-same-thread", "", submitInSameThread, false);
    Thread.sleep(2);
    submitInSameThread.firstUnblock();
    Thread.sleep(10);
    submitInSameThread.secondUnblock();

    future.get();
  }

  private static class TaskSubmissionThread extends Thread {
    private final List<CommandPool.Command<?>> commands;
    private final ReleasableCommandPool commandPool;
    private final int threadId;

    TaskSubmissionThread(List<CommandPool.Command<?>> commands, ReleasableCommandPool commandPool, int threadId) {
      this.commands = commands;
      this.commandPool = commandPool;
      this.threadId = threadId;
    }

    @Override
    public void run() {
      for(CommandPool.Command<?> command : commands) {
        try {
          commandPool.submit(CommandPool.Priority.HIGH, "thread-" + threadId, "", command, false).get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private class CountMaxAndCheckThreadIds extends StartAndStop {
    private int count = 0;
    private int maxCount = 0;
    private final ReleasableBoundCommandPool commandPool;

    CountMaxAndCheckThreadIds(ReleasableBoundCommandPool commandPool) {
      this.commandPool = commandPool;
    }

    @Override
    void start() {
      long threadId = Thread.currentThread().getId();
      Assert.assertTrue(commandPool.getThreadsHoldingSlots().contains(threadId));
      synchronized (this) {
        count++;
        if (count > maxCount) {
          maxCount = count;
        }
      }
    }

    @Override
    void stop() {
      long threadId = Thread.currentThread().getId();
      Assert.assertTrue(commandPool.getThreadsHoldingSlots().contains(threadId));
      synchronized (this) {
        count--;
      }
    }
  }

  private interface ReleasingCommand extends CommandPool.Command<Integer> {
    void firstUnblock();
    default void secondUnblock() {}
  }

  private class SimpleBlockingCommand implements ReleasingCommand {
    private Semaphore first = new Semaphore(0);
    private ReleasableBoundCommandPool releasableBoundCommandPool;
    private final StartAndStop startAndStop;

    SimpleBlockingCommand(ReleasableBoundCommandPool releasableBoundCommandPool, StartAndStop startAndStop) {
      this.releasableBoundCommandPool = releasableBoundCommandPool;
      this.startAndStop = startAndStop;
    }

    @Override
    public Integer get(long waitInMillis) throws Exception {
      startAndStop.start();
      first.acquire();
      startAndStop.stop();
      return counter.incrementAndGet();
    }

    @Override
    public void firstUnblock() {
      first.release();
    }
  }

  private class ReleaseAndWaitCommand implements ReleasingCommand {
    private Semaphore first = new Semaphore(0);
    private Semaphore second = new Semaphore(0);
    private ReleasableBoundCommandPool releasableBoundCommandPool;
    private StartAndStop startAndStop;

    ReleaseAndWaitCommand(ReleasableBoundCommandPool releasableBoundCommandPool, StartAndStop startAndStop) {
      this.releasableBoundCommandPool = releasableBoundCommandPool;
      this.startAndStop = startAndStop;
    }

    @Override
    public Integer get(long waitInMillis) throws Exception {
      try {
        startAndStop.start();
        long threadId = Thread.currentThread().getId();
        first.acquire();
        Assert.assertTrue(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
        startAndStop.stop();
        try(Closeable closeable = releasableBoundCommandPool.releaseAndReacquireSlot()) {
          Assert.assertFalse(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
          second.acquire();
        }
        startAndStop.start();
        Assert.assertTrue(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
        startAndStop.stop();
        return counter.incrementAndGet();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void firstUnblock() {
      first.release();
    }

    @Override
    public void secondUnblock() {
      second.release();
    }
  }

  private class ReleaseAndResubmitTask implements ReleasingCommand {
    private Semaphore first = new Semaphore(0);
    private BlockingCommand commandToSubmit;
    private ReleasableBoundCommandPool releasableBoundCommandPool;
    private StartAndStop startAndStop;

    ReleaseAndResubmitTask(ReleasableBoundCommandPool releasableBoundCommandPool, StartAndStop startAndStop) {
      this.releasableBoundCommandPool = releasableBoundCommandPool;
      this.startAndStop = startAndStop;
      this.commandToSubmit = new BlockingCommand(startAndStop);
    }

    @Override
    public Integer get(long waitInMillis) throws Exception {
      try {
        startAndStop.start();
        long threadId = Thread.currentThread().getId();
        first.acquire();
        Assert.assertTrue(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
        startAndStop.stop();
        try(Closeable closeable = releasableBoundCommandPool.releaseAndReacquireSlot()) {
          Assert.assertFalse(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
          releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "submitted-task", "", commandToSubmit, false).get();
        }
        startAndStop.start();
        Assert.assertTrue(releasableBoundCommandPool.getThreadsHoldingSlots().contains(threadId));
        startAndStop.stop();
        return counter.incrementAndGet();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void firstUnblock() {
      first.release();
    }

    @Override
    public void secondUnblock() {
      commandToSubmit.unblock();
    }
  }

  private class SubmitInSameThread implements ReleasingCommand {
    private final Semaphore first = new Semaphore(0);
    private final BlockingCommand cmd = new BlockingCommand(new StartAndStop());
    private final ReleasableBoundCommandPool releasableBoundCommandPool;

    SubmitInSameThread(ReleasableBoundCommandPool releasableBoundCommandPool) {
      this.releasableBoundCommandPool = releasableBoundCommandPool;
    }

    @Override
    public Integer get(long waitInMillis) throws Exception {
      first.acquire();
      releasableBoundCommandPool.submit(CommandPool.Priority.HIGH, "task-same-thread", "", cmd, true).get();
      return 0;
    }

    @Override
    public void firstUnblock() {
      first.release();
    }

    @Override
    public void secondUnblock() {
      cmd.unblock();
    }
  }
}
