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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.DeferredException;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.service.grpc.OnReadyRunnableHandler;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Throwables;

import io.grpc.stub.ServerCallStreamObserver;
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
class JobEventCollatingObserver implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(JobEventCollatingObserver.class);

  private static final QueuedEvent SENT = new QueuedEvent();
  private static final int DATA_ORDER_INDEX = 3;

  private final QueuedEvent[] checkpointEvents = new QueuedEvent[5];
  private final LinkedList<QueuedEvent> otherEvents = new LinkedList<>();

  private final JobId jobId;
  private final StreamObserver<JobEvent> delegate;
  private final ServerCallStreamObserver<JobEvent> scso;

  private volatile boolean started = false;
  private volatile boolean closed = false;

  JobEventCollatingObserver(JobId jobId,
                            StreamObserver<JobEvent> delegate,
                            CloseableExecutorService executorService,
                            boolean streamResultsMode) {
    this.jobId = jobId;
    this.delegate = delegate;

    if (!streamResultsMode) {
      checkpointEvents[DATA_ORDER_INDEX] = SENT;
    }

    // backpressure is supported only if delegate is instance of ServerCallStreamObserver,
    // as isReady() & setOnReadyHandler() methods are required to detect client readiness to accept messages.
    if (delegate instanceof ServerCallStreamObserver) {
      logger.debug("Flow control is enabled for job {}.", jobId);
      this.scso = (ServerCallStreamObserver<JobEvent>) delegate;

      OnReadyRunnableHandler eventHandler = new OnReadyRunnableHandler("job-events", executorService,
        scso, () -> processEvents(), () -> closed = true);
      this.scso.setOnReadyHandler(eventHandler);
      this.scso.setOnCancelHandler(eventHandler::cancel);
    } else {
      this.scso = null;
    }
  }

  /**
   * Starts processing events.
   *
   * @param jobSubmissionEvent the first event to process
   */
  synchronized void start(JobEvent jobSubmissionEvent) {
    checkpointEvents[0] = new QueuedEvent(jobSubmissionEvent);
    started = true;

    processEvents();
  }

  void onSubmitted(JobEvent event) {
    checkpoint(new QueuedEvent(event), 1);
  }

  void onQueryMetadata(JobEvent event) {
    checkpoint(new QueuedEvent(event), 2);
  }

  void onData(JobEvent event, RpcOutcomeListener<Ack> outcomeListener) {
    if (checkpointEvents[DATA_ORDER_INDEX] == null) {
      synchronized (this) {
        if (checkpointEvents[DATA_ORDER_INDEX] == null) {
          QueuedEvent dataEvent = new QueuedEvent();
          dataEvent.addData(event, outcomeListener);
          checkpoint(dataEvent, DATA_ORDER_INDEX);
        }
      }
    } else {
      checkpointEvents[DATA_ORDER_INDEX].addData(event, outcomeListener);
      if (started && mayProcess(DATA_ORDER_INDEX)) {
        processEvents();
      }
    }
  }

  void onFinalJobSummary(JobEvent event) {
    checkpoint(new QueuedEvent(event), 4);
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

  private synchronized void processEvents() {
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
        if (i == DATA_ORDER_INDEX) {
          while (isClientReady() && !checkpointEvents[DATA_ORDER_INDEX].getDataQueue().isEmpty()) {
            Pair<JobEvent, RpcOutcomeListener<Ack>> data = checkpointEvents[DATA_ORDER_INDEX].getDataQueue().poll();
            if (data != null) {
              delegate.onNext(data.getLeft());

              // The executor has a cap on the number of outstanding acks for data batches,
              // and will stop sending more batches if that cap is reached.
              // So, sending the ack to executor only after the data batch is picked up by the client
              // works as flow-control for the executor->coordinator communication.
              data.getRight().success(Acks.OK, null);
            }
          }
          if (!checkpointEvents[DATA_ORDER_INDEX].getDataQueue().isEmpty()) {
            break;
          }
        } else {
          delegate.onNext(event.event);
        }
      } catch (Exception e) {
        deferredException.addException(e);
      }

      if (i != DATA_ORDER_INDEX) {
        checkpointEvents[i] = SENT;

        // there can be multiple data events, so set data as SENT only when next event to it was sent.
        if (i == (DATA_ORDER_INDEX + 1)) {
          checkpointEvents[DATA_ORDER_INDEX] = SENT;
        }
      }
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

  private boolean isClientReady() {
    return scso == null || scso.isReady();
  }

  @Override
  public void close() throws Exception { }

  /**
   * Queued up event.
   */
  private static final class QueuedEvent {
    private final JobEvent event;
    private final Throwable t;
    private Queue<Pair<JobEvent, RpcOutcomeListener<Ack>>> data;

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

    void addData(JobEvent event, RpcOutcomeListener<Ack> outcomeListener) {
      if (data == null) {
        data = new ConcurrentLinkedDeque<>();
      }
      data.offer(Pair.of(event, outcomeListener));
    }

    Queue<Pair<JobEvent, RpcOutcomeListener<Ack>>> getDataQueue() {
      return data;
    }
  }
}
