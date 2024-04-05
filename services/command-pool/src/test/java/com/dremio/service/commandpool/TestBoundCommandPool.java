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

import com.google.common.util.concurrent.Futures;
import io.opentracing.noop.NoopTracerFactory;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link BoundCommandPool} */
public class TestBoundCommandPool {

  private final AtomicInteger counter = new AtomicInteger();

  CommandPool newTestCommandPool() {
    return new BoundCommandPool(1, NoopTracerFactory.create());
  }

  @Before
  public void resetCounter() {
    counter.set(0);
  }

  @Test
  public void testSamePrioritySuppliers() throws Exception {
    // single threaded pool to have a deterministic ordering of execution
    final CommandPool pool = newTestCommandPool();

    final BlockingCommand blocking = new BlockingCommand(new StartAndStop());
    pool.submit(CommandPool.Priority.HIGH, "test", "test", blocking, false);

    // submitting multiple suppliers with the same priority should be executed in their submitted
    // order
    Future<Integer> future1 =
        pool.submit(
            CommandPool.Priority.HIGH,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);
    Thread.sleep(5);
    Future<Integer> future2 =
        pool.submit(
            CommandPool.Priority.HIGH,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);
    Thread.sleep(5);
    Future<Integer> future3 =
        pool.submit(
            CommandPool.Priority.HIGH,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);

    blocking.unblock();
    Assert.assertEquals(0, (int) Futures.getUnchecked(future1));
    Assert.assertEquals(1, (int) Futures.getUnchecked(future2));
    Assert.assertEquals(2, (int) Futures.getUnchecked(future3));
  }

  @Test
  public void testDifferentPrioritySuppliers() {
    // single threaded pool to have a deterministic ordering of execution
    final CommandPool pool = newTestCommandPool();

    final BlockingCommand blocking = new BlockingCommand(new StartAndStop());
    pool.submit(CommandPool.Priority.HIGH, "test", "test", blocking, false);

    // submitting multiple suppliers with different priorities, to a single thread pool, should be
    // executed according to their priority
    Future<Integer> future1 =
        pool.submit(
            CommandPool.Priority.LOW,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);
    Future<Integer> future2 =
        pool.submit(
            CommandPool.Priority.MEDIUM,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);
    Future<Integer> future3 =
        pool.submit(
            CommandPool.Priority.HIGH,
            "test",
            "test",
            (waitInMillis) -> counter.getAndIncrement(),
            false);

    blocking.unblock();
    Assert.assertEquals(2, (int) Futures.getUnchecked(future1));
    Assert.assertEquals(1, (int) Futures.getUnchecked(future2));
    Assert.assertEquals(0, (int) Futures.getUnchecked(future3));
  }

  /** Runnable that starts in a blocked state and can be unblocked */
  protected static final class BlockingCommand implements CommandPool.Command<Void> {
    private final Semaphore semaphore = new Semaphore(0);
    private final StartAndStop startAndStop;

    BlockingCommand(StartAndStop startAndStop) {
      this.startAndStop = startAndStop;
    }

    void unblock() {
      semaphore.release();
    }

    @Override
    public Void get(long waitInMillis) {
      try {
        startAndStop.start();
        semaphore.acquire();
        startAndStop.stop();
        return null;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected static class StartAndStop {
    void start() {}

    void stop() {}
  }
}
