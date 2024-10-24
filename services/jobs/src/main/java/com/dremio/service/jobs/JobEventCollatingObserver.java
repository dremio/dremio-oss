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

import static com.dremio.service.jobs.JobEventCollatingObserver.EventType.DATA;
import static com.dremio.service.jobs.JobEventCollatingObserver.EventType.FINAL_JOB_SUMMARY;
import static com.dremio.service.jobs.JobEventCollatingObserver.EventType.JOB_SUBMISSION;
import static com.dremio.service.jobs.JobEventCollatingObserver.EventType.JOB_SUBMITTED;
import static com.dremio.service.jobs.JobEventCollatingObserver.EventType.METADATA;

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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream observer wrapper that ensures: (1) events are sent in the correct order. (2) all
 * checkpoint events are sent before the stream is closed.
 *
 * <p>If there is an exception, delivery of all events is not guaranteed.
 *
 * <p>Since this object is not highly contended, the implementation uses coarse-grained locking.
 */
@ThreadSafe
class JobEventCollatingObserver implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(JobEventCollatingObserver.class);

  private static final QueuedEvent SENT = new QueuedEvent();

  enum EventType {
    JOB_SUBMISSION(0),
    JOB_SUBMITTED(1),
    METADATA(2),
    DATA(3),
    FINAL_JOB_SUMMARY(4);

    private final int index;

    EventType(final int index) {
      this.index = index;
    }

    @Override
    public String toString() {
      return super.toString() + "(" + index + ")";
    }

    public int getIndex() {
      return index;
    }
  }

  private final QueuedEvent[] checkpointEvents = new QueuedEvent[5];
  private final LinkedList<QueuedEvent> otherEvents = new LinkedList<>();

  private final JobId jobId;
  private final StreamObserver<JobEvent> delegate;
  private final ServerCallStreamObserver<JobEvent> scso;

  private volatile boolean started = false;
  private volatile boolean closed = false;

  JobEventCollatingObserver(
      JobId jobId,
      StreamObserver<JobEvent> delegate,
      CloseableExecutorService executorService,
      boolean streamResultsMode) {
    this.jobId = jobId;
    this.delegate = delegate;

    if (!streamResultsMode) {
      checkpointEvents[DATA.getIndex()] = SENT;
    }

    // backpressure is supported only if delegate is instance of ServerCallStreamObserver,
    // as isReady() & setOnReadyHandler() methods are required to detect client readiness to accept
    // messages.
    if (delegate instanceof ServerCallStreamObserver) {
      logger.debug("Flow control is enabled for job {}.", jobId);
      this.scso = (ServerCallStreamObserver<JobEvent>) delegate;

      OnReadyRunnableHandler<JobEvent> eventHandler =
          new OnReadyRunnableHandler<>(
              "job-events", executorService, scso, this::processEvents, () -> closed = true);
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
    checkpointEvents[JOB_SUBMISSION.getIndex()] = new QueuedEvent(jobSubmissionEvent);
    started = true;

    processEvents();
  }

  void onSubmitted(JobEvent event) {
    checkpoint(new QueuedEvent(event), JOB_SUBMITTED);
  }

  void onQueryMetadata(JobEvent event) {
    checkpoint(new QueuedEvent(event), METADATA);
  }

  void onData(JobEvent event, RpcOutcomeListener<Ack> outcomeListener) {
    if (checkpointEvents[DATA.getIndex()] == null) {
      synchronized (this) {
        if (checkpointEvents[DATA.getIndex()] == null) {
          QueuedEvent dataEvent = new QueuedEvent();
          dataEvent.addData(event, outcomeListener);
          checkpoint(dataEvent, DATA);
        }
      }
    } else {
      checkpointEvents[DATA.getIndex()].addData(event, outcomeListener);
      if (started && mayProcess(DATA)) {
        processEvents();
      }
    }
  }

  void onFinalJobSummary(JobEvent event) {
    checkpoint(new QueuedEvent(event), FINAL_JOB_SUMMARY);
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

  private synchronized void checkpoint(QueuedEvent event, EventType type) {
    assert checkpointEvents[type.getIndex()] == null;
    checkpointEvents[type.getIndex()] = event;
    if (!mayProcess(type)) {
      return;
    }
    if (!started) {
      logger.debug("{}: deferring checkpoint processing since the stream is not ready", jobId);
      return;
    }

    processEvents();
  }

  private boolean mayProcess(EventType type) {
    boolean mayProcess = true;
    for (EventType t : EventType.values()) {
      if (t == type) {
        break;
      }
      if (checkpointEvents[t.getIndex()] == null) {
        if (type == DATA || type == FINAL_JOB_SUMMARY) {
          logger.warn("{}: cannot process {} since preceding {} is null", jobId, type, t);
        } else {
          logger.debug("{}: cannot process {} since preceding {} is null", jobId, type, t);
        }
        mayProcess = false;
        break;
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
    for (EventType t : EventType.values()) {
      final QueuedEvent event = checkpointEvents[t.getIndex()];
      if (event == null) {
        break;
      }
      //noinspection ObjectEquality
      if (event == SENT) {
        continue;
      }

      try {
        if (t == DATA) {
          while (isClientReady() && !checkpointEvents[DATA.getIndex()].getDataQueue().isEmpty()) {
            Pair<JobEvent, RpcOutcomeListener<Ack>> data =
                checkpointEvents[DATA.getIndex()].getDataQueue().poll();
            if (data != null) {
              delegate.onNext(data.getLeft());

              // The executor has a cap on the number of outstanding acks for data batches,
              // and will stop sending more batches if that cap is reached.
              // So, sending the ack to executor only after the data batch is picked up by the
              // client
              // works as flow-control for the executor->coordinator communication.
              data.getRight().success(Acks.OK, null);
            }
          }
          if (!checkpointEvents[DATA.getIndex()].getDataQueue().isEmpty()) {
            break;
          }
        } else {
          delegate.onNext(event.event);
        }
      } catch (Exception e) {
        deferredException.addException(e);
      }

      if (t != DATA) {
        checkpointEvents[t.getIndex()] = SENT;

        // there can be multiple data events, so set data as SENT only when next event to it was
        // sent.
        if (t.getIndex() == (DATA.getIndex() + 1)) {
          checkpointEvents[DATA.getIndex()] = SENT;
        }
      }
    }

    // (2) then, handle all other events
    while (!otherEvents.isEmpty()) {
      final QueuedEvent event = otherEvents.removeFirst();
      if (closed) {
        logger.debug(
            "{}: flushing event ({}, {}) since the stream is closed",
            jobId,
            event.event == null,
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
    return Arrays.stream(checkpointEvents).allMatch(event -> event == SENT);
  }

  private boolean isClientReady() {
    return scso == null || scso.isReady();
  }

  @Override
  public void close() throws Exception {}

  /** Queued up event. */
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
