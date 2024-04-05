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
package org.apache.arrow.flight;

import com.google.common.base.Preconditions;

public class DremioBackpressureStrategy implements BackpressureStrategy {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioBackpressureStrategy.class);
  private final Object lock = new Object();
  private FlightProducer.ServerStreamListener listener;
  private boolean ready;
  private boolean cancelled;

  @Override
  public void register(FlightProducer.ServerStreamListener listener) {
    this.listener = listener;
    listener.setOnReadyHandler(this::onReady);
    listener.setOnCancelHandler(this::onCancel);
  }

  @Override
  public WaitResult waitForListener(long timeout) {
    Preconditions.checkNotNull(listener);
    long remainingTimeout = timeout;
    final long startTime = System.currentTimeMillis();
    synchronized (lock) {
      ready = listener.isReady();
      cancelled = listener.isCancelled();

      logger.info(">>> BackPressure.isReady() = " + ready);

      while (!ready && !cancelled) {
        try {
          lock.wait(remainingTimeout);
          if (timeout != 0) { // If timeout was zero explicitly, we should never report timeout.
            remainingTimeout = startTime + timeout - System.currentTimeMillis();
            if (remainingTimeout <= 0) {
              return WaitResult.TIMEOUT;
            }
          }
          if (!shouldContinueWaiting(listener, remainingTimeout)) {
            return WaitResult.OTHER;
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          return WaitResult.OTHER;
        }
      }

      if (ready) {
        return WaitResult.READY;
      } else if (cancelled) {
        return WaitResult.CANCELLED;
      } else if (System.currentTimeMillis() > startTime + timeout) {
        return WaitResult.TIMEOUT;
      }
      throw new RuntimeException("Invalid state when waiting for listener.");
    }
  }

  /**
   * Interrupt waiting on the listener to change state.
   *
   * <p>This method can be used in conjunction with {@link
   * #shouldContinueWaiting(FlightProducer.ServerStreamListener, long)} to allow FlightProducers to
   * terminate streams internally and notify clients.
   */
  public void interruptWait() {
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  /**
   * Callback function to run to check if the listener should continue to be waited on if it leaves
   * the waiting state without being cancelled, ready, or timed out.
   *
   * <p>This method should be used to determine if the wait on the listener was interrupted
   * explicitly using a call to {@link #interruptWait()} or if it was woken up due to a spurious
   * wake.
   */
  protected boolean shouldContinueWaiting(
      FlightProducer.ServerStreamListener listener, long remainingTimeout) {
    return true;
  }

  /** Callback to execute when the listener becomes ready. */
  protected void readyCallback() {}

  /** Callback to execute when the listener is cancelled. */
  protected void cancelCallback() {}

  private void onReady() {
    synchronized (lock) {
      ready = true;
      readyCallback();
      lock.notifyAll();
    }
  }

  private void onCancel() {
    synchronized (lock) {
      cancelled = true;
      cancelCallback();
      lock.notifyAll();
    }
  }
}
