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
package com.dremio.service.grpc;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.SerializedExecutor;

import io.grpc.stub.ServerCallStreamObserver;

/**
  * Abstract handler which is invoked every time the peer is ready to receive more messages.
  *
  * @param <V> response type
  */
public class OnReadyRunnableHandler<V> implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OnReadyRunnableHandler.class);

  private final OnReadyEventExecutor executor;
  private final ServerCallStreamObserver<V> responseObserver;

  private final Runnable onReadyRunnable;
  private final Runnable onCancelRunnable;

  public OnReadyRunnableHandler(
    String requestType,
    Executor executor,
    ServerCallStreamObserver<V> streamObserver,
    Runnable onReadyRunnable,
    Runnable onCancelRunnable
  ) {
    this.executor = new OnReadyEventExecutor(requestType, executor);
    this.responseObserver = streamObserver;
    this.onReadyRunnable = onReadyRunnable;
    this.onCancelRunnable = onCancelRunnable;
  }

  public void cancel() {
    onCancelRunnable.run();
  }

  @Override
  public void run() {
    if (!responseObserver.isReady()) {
      // see CallStreamObserver#setOnReadyHandler
      // handle spurious notifications: although handled in handleStreamReady, this avoids volatile reads
      return;
    }

    executor.execute(onReadyRunnable);
  }

  /**
   * Serializes execution of {@code #onReady} events, and offloads request handling.
   * <p>
   * This ensures there is no write contention (including errors).
   */
  private final class OnReadyEventExecutor extends SerializedExecutor<Runnable> {

    private OnReadyEventExecutor(String requestType, Executor underlyingExecutor) {
      super(requestType, underlyingExecutor, false);
    }

    @Override
    protected void runException(Runnable command, Throwable t) {
      responseObserver.onError(t);
      onCancelRunnable.run();
    }
  }
}
