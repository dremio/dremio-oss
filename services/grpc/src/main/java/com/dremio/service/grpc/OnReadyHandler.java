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

import com.dremio.common.SerializedExecutor;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Iterator;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract handler which is invoked every time the peer is ready to receive more messages.
 *
 * @param <V> response type
 */
public abstract class OnReadyHandler<V> implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OnReadyHandler.class);

  private final OnReadyEventExecutor executor;
  private final ServerCallStreamObserver<V> responseObserver;

  private volatile Iterator<V> iterator;

  public OnReadyHandler(
      String requestType,
      Executor executor,
      ServerCallStreamObserver<V> streamObserver,
      Iterator<V> iterator) {
    this.executor = new OnReadyEventExecutor(requestType, executor);
    this.responseObserver = streamObserver;

    this.iterator = iterator;
  }

  public void cancel() {
    // clear state
    iterator = null;
  }

  @Override
  public void run() {
    if (iterator == null) {
      logger.debug("Callback was invoked even though last message was sent");
      return;
    }

    if (!responseObserver.isReady()) {
      // see CallStreamObserver#setOnReadyHandler
      // handle spurious notifications: although handled in handleStreamReady, this avoids volatile
      // reads
      return;
    }

    executor.execute(this::handleStreamReady);
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private void handleStreamReady() {
    // Every run try to send as many responses as the client is willing to receive. This also
    // handles
    // cancellation and depleting enqueued requests in SerializedExecutor#queuedRunnables.

    Iterator<V> iterator = this.iterator;
    int numSent = 0;

    while (responseObserver.isReady() && iterator != null && iterator.hasNext()) {
      try {
        responseObserver.onNext(iterator.next());
      } catch (Exception e) {
        responseObserver.onError(e);
        this.iterator = null;
        return;
      }

      numSent++;
      if (numSent == 500) { // refresh iterator
        iterator = this.iterator;
        numSent = 0;
      }
    }

    iterator = this.iterator;
    if (iterator != null && !iterator.hasNext()) {
      responseObserver.onCompleted();
      this.iterator = null;
    }
  }

  /**
   * Serializes execution of {@code #onReady} events, and offloads request handling.
   *
   * <p>This ensures there is no write contention (including errors).
   */
  private final class OnReadyEventExecutor extends SerializedExecutor<Runnable> {

    private OnReadyEventExecutor(String requestType, Executor underlyingExecutor) {
      super(requestType, underlyingExecutor, false);
    }

    @Override
    @SuppressWarnings("DremioGRPCStreamObserverOnError")
    protected void runException(Runnable command, Throwable t) {
      responseObserver.onError(t);
      iterator = null;
    }
  }
}
