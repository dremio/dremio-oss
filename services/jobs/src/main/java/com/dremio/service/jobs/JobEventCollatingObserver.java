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
package com.dremio.service.jobs;

import java.util.Arrays;
import java.util.LinkedList;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.DeferredException;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Throwables;

import io.grpc.stub.StreamObserver;

/**
 * Stream observer wrapper that ensures:
 * (1) events are sent in the correct order.
 * (2) all checkpoint events are sent before the stream is closed.
 * <p>
 * If there is an exception, delivery of all events is not guaranteed.
 * <p>
 * Since this object is not highly contended, the implementation uses coarse-grained locking.
 */
@ThreadSafe
class JobEventCollatingObserver {
  private static final Logger logger = LoggerFactory.getLogger(JobEventCollatingObserver.class);

  private static final QueuedEvent SENT = new QueuedEvent();

  private final QueuedEvent[] checkpointEvents = new QueuedEvent[4];
  private final LinkedList<QueuedEvent> otherEvents = new LinkedList<>();

  private final JobId jobId;
  private final StreamObserver<JobEvent> delegate;

  private volatile boolean started = false;
  private volatile boolean closed = false;

  JobEventCollatingObserver(JobId jobId, StreamObserver<JobEvent> delegate) {
    this.jobId = jobId;
    this.delegate = delegate;
  }

  /**
   * Starts processing events.
   *
   * @param jobIdEvent the first event to process
   */
  synchronized void start(JobEvent jobIdEvent) {
    checkpointEvents[0] = new QueuedEvent(jobIdEvent);
    started = true;

    processEvents();
  }

  void onSubmitted(JobEvent event) {
    checkpoint(new QueuedEvent(event), 1);
  }

  void onQueryMetadata(JobEvent event) {
    checkpoint(new QueuedEvent(event), 2);
  }

  void onFinalJobSummary(JobEvent event) {
    checkpoint(new QueuedEvent(event), 3);
  }

  void onProgressJobSummary(JobEvent event) {
    submit(new QueuedEvent(event));
  }

  void onError(Throwable t) {
    submit(new QueuedEvent(t));
  }

  void onCompleted() {
    submit(new QueuedEvent());
  }

  private synchronized void checkpoint(QueuedEvent event, int index) {
    assert checkpointEvents[index] == null;
    checkpointEvents[index] = event;
    if (!mayProcess(index) || !started) {
      logger.debug("{}: deferring checkpoint processing since the stream is not ready", jobId);
      return;
    }

    processEvents();
  }

  private boolean mayProcess(final int index) {
    boolean mayProcess = true;
    for (int i = 0; i < index; i++) {
      if (checkpointEvents[i] == null) {
        logger.debug("{}: cannot process {} since preceding {} is null", jobId, index, i);
        mayProcess = false;
      }
    }
    return mayProcess;
  }

  private synchronized void submit(QueuedEvent event) {
    if (closed) {
      logger.debug("{}: dropping event since the stream is closed", jobId);
      return;
    }

    otherEvents.addLast(event);
    if (!started) {
      logger.debug("{}: deferring event processing since the stream has not started", jobId);
      return;
    }

    processEvents();
  }

  private void processEvents() {
    @SuppressWarnings("resource")
    final DeferredException deferredException = new DeferredException();

    // (1) pass through and handle all the checkpoint events, if possible
    for (int i = 0; i < checkpointEvents.length; i++) {
      final QueuedEvent event = checkpointEvents[i];
      if (event == null) {
        break;
      }
      //noinspection ObjectEquality
      if (event == SENT) {
        continue;
      }

      try {
        delegate.onNext(event.event);
      } catch (Exception e) {
        deferredException.addException(e);
      }

      checkpointEvents[i] = SENT;
    }

    // (2) then, handle all other events
    while (!otherEvents.isEmpty()) {
      final QueuedEvent event = otherEvents.removeFirst();
      if (closed) {
        logger.debug("{}: flushing event ({}, {}) since the stream is closed", jobId, event.event == null,
            event.t == null);
        continue;
      }

      try {
        if (event.event != null) {
          delegate.onNext(event.event);
        } else if (event.t != null) {
          // on error, stop sending events over stream
          JobsRpcUtils.handleException(delegate, event.t);
          closed = true;
        } else if (mayClose()) {
          // only if all checkpoint events happen, close the stream
          delegate.onCompleted();
          closed = true;
        } else {
          otherEvents.addLast(event);
          break; // process again later
        }
      } catch (Exception e) {
        deferredException.addException(e);
      }
    }

    try {
      deferredException.close();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException("Exceptions caught during event processing", e);
    }
  }

  private boolean mayClose() {
    //noinspection ObjectEquality
    return Arrays.stream(checkpointEvents)
        .allMatch(event -> event == SENT);
  }

  /**
   * Queued up event.
   */
  private static final class QueuedEvent {
    private final JobEvent event;
    private final Throwable t;

    QueuedEvent(JobEvent event) {
      this.event = event;
      this.t = null;
    }

    QueuedEvent(Throwable t) {
      this.event = null;
      this.t = t;
    }

    QueuedEvent() {
      this.event = null;
      this.t = null;
    }
  }
}
