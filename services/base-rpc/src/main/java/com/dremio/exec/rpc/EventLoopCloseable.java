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
package com.dremio.exec.rpc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Stopwatch;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.TimeUnit;

public class EventLoopCloseable implements AutoCloseable {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(EventLoopCloseable.class);

  private final EventLoopGroup eventLoop;

  public EventLoopCloseable(EventLoopGroup eventLoop) {
    super();
    this.eventLoop = eventLoop;
  }

  @Override
  public void close() throws Exception {
    try {
      Stopwatch watch = Stopwatch.createStarted();
      // this takes 1s to complete
      // known issue: https://github.com/netty/netty/issues/2545
      eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      eventLoop.terminationFuture().sync();

      long elapsed = watch.elapsed(MILLISECONDS);
      if (elapsed > 1200) {
        logger.info("closed eventLoopGroups in " + elapsed + " ms");
      }
    } catch (final InterruptedException e) {
      logger.warn("Failure while shutting down bootstrap context event loops.", e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack
      // can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }
}
