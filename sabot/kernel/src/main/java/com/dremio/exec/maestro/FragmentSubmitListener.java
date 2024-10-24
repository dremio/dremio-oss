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
package com.dremio.exec.maestro;

import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ExecutionControls;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.google.protobuf.MessageLite;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class FragmentSubmitListener implements StreamObserver<Empty> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FragmentSubmitListener.class);
  private final CountDownLatch latch;
  private final FragmentSubmitFailures fragmentSubmitFailures;
  private final FragmentSubmitSuccess fragmentSubmitSuccesses;
  private final CoordinationProtos.NodeEndpoint endpoint;
  private final AtomicBoolean done;
  private final MaestroObserver observer;
  private final ExecutionControls executionControls;
  private final ControlsInjector injector;

  /**
   * Constructor.
   *
   * @param endpoint the endpoint for the submission
   * @param value the initialize fragments message
   * @param latch the latch to count down when the status is known; may be null
   * @param fragmentSubmitFailures the counter to use for failures; must be non-null iff latch is
   *     non-null
   */
  public FragmentSubmitListener(
      MaestroObserver observer,
      final CoordinationProtos.NodeEndpoint endpoint,
      final MessageLite value,
      final CountDownLatch latch,
      final FragmentSubmitFailures fragmentSubmitFailures,
      final FragmentSubmitSuccess fragmentSubmitSuccess,
      final ExecutionControls executionControls,
      final ControlsInjector injector) {
    Preconditions.checkState((latch == null) == (fragmentSubmitFailures == null));
    this.latch = latch;
    this.fragmentSubmitFailures = fragmentSubmitFailures;
    this.endpoint = endpoint;
    this.fragmentSubmitSuccesses = fragmentSubmitSuccess;
    done = new AtomicBoolean(false);
    this.observer = observer;
    this.executionControls = executionControls;
    this.injector = injector;
  }

  @Override
  public void onNext(Empty empty) {
    // no op
  }

  @Override
  public void onError(Throwable throwable) {
    if (latch != null
        && done.compareAndSet(false, true)) { // this block only applies to start rpcs.
      RpcException ex = RpcException.mapException(throwable);
      fragmentSubmitFailures.addFailure(endpoint, ex);
      latch.countDown();
    } else { // this block only applies to activate rpcs.
      if (observer != null) {
        observer.activateFragmentFailed(
            new RpcException(
                String.format(
                    "Failure sending activate " + "rpc for fragments to %s:%d.",
                    endpoint.getAddress(), endpoint.getFabricPort()),
                throwable));
      }
    }
  }

  @Override
  public void onCompleted() {

    if (injector != null) {
      injector.injectPause(
          executionControls, FragmentStarter.INJECTOR_AFTER_ON_COMPLETED_PAUSE, logger);
    }

    if (latch != null && done.compareAndSet(false, true)) {
      fragmentSubmitSuccesses.addSuccess(endpoint);
      latch.countDown();
    }
  }

  public static void awaitUninterruptibly(
      List<CoordinationProtos.NodeEndpoint> endPointIndexList,
      ExtendedLatch endpointLatch,
      FragmentSubmitFailures fragmentSubmitFailures,
      FragmentSubmitSuccess fragmentSubmitSuccess,
      int numNodes,
      long timeout) {
    if (numNodes > 0 && !endpointLatch.awaitUninterruptibly(timeout)) {
      long numberRemaining = endpointLatch.getCount();
      StringBuilder sb = new StringBuilder();
      for (final CoordinationProtos.NodeEndpoint ep : endPointIndexList) {
        if (!fragmentSubmitSuccess.getSubmissionSuccesses().contains(ep)
            && !fragmentSubmitFailures.listContains(ep)) {
          // The fragment sent to this endPoint timed out.
          if (sb.length() != 0) {
            sb.append(", ");
          }
          sb.append(ep.getAddress());
        }
      }
      throw UserException.connectionError()
          .message(
              "Exceeded timeout (%d) while waiting after sending work fragments to remote nodes. "
                  + "Sent %d and only heard response back from %d nodes",
              timeout, numNodes, numNodes - numberRemaining)
          .addContext("Node(s) that did not respond", sb.toString())
          .build(logger);
    }
  }

  public static void checkForExceptions(FragmentSubmitFailures fragmentSubmitFailures) {
    // if any of the fragment submissions failed, fail the query
    final List<FragmentSubmitFailures.SubmissionException> submissionExceptions =
        fragmentSubmitFailures.getSubmissionExceptions();
    if (!submissionExceptions.isEmpty()) {
      Set<CoordinationProtos.NodeEndpoint> endpoints = Sets.newHashSet();
      StringBuilder sb = new StringBuilder();

      for (FragmentSubmitFailures.SubmissionException e :
          fragmentSubmitFailures.getSubmissionExceptions()) {
        CoordinationProtos.NodeEndpoint endpoint = e.getNodeEndpoint();
        if (endpoints.add(endpoint)) {
          if (sb.length() != 0) {
            sb.append(", ");
          }
          sb.append(endpoint.getAddress());
        }
      }
      throw UserException.connectionError(submissionExceptions.get(0).getRpcException())
          .message("Error setting up remote fragment execution")
          .addContext("Nodes with failures", sb.toString())
          .build(logger);
    }
  }

  /** Used by {@link FragmentSubmitListener} to track the number of submission failures. */
  public static final class FragmentSubmitFailures {
    public static class SubmissionException {
      final CoordinationProtos.NodeEndpoint nodeEndpoint;
      final RpcException rpcException;

      SubmissionException(
          final CoordinationProtos.NodeEndpoint nodeEndpoint, final RpcException rpcException) {
        this.nodeEndpoint = nodeEndpoint;
        this.rpcException = rpcException;
      }

      public CoordinationProtos.NodeEndpoint getNodeEndpoint() {
        return nodeEndpoint;
      }

      public RpcException getRpcException() {
        return rpcException;
      }
    }

    final List<SubmissionException> submissionExceptions =
        Collections.synchronizedList(new LinkedList<>());

    public List<SubmissionException> getSubmissionExceptions() {
      return submissionExceptions;
    }

    public void addFailure(
        final CoordinationProtos.NodeEndpoint nodeEndpoint, final RpcException rpcException) {
      submissionExceptions.add(new SubmissionException(nodeEndpoint, rpcException));
    }

    public boolean listContains(final CoordinationProtos.NodeEndpoint nodeEndpoint) {
      for (SubmissionException se : submissionExceptions) {
        if (se.nodeEndpoint.equals(nodeEndpoint)) {
          return true;
        }
      }
      return false;
    }
  }

  public static final class FragmentSubmitSuccess {
    final List<CoordinationProtos.NodeEndpoint> submissionSuccesses =
        Collections.synchronizedList(new LinkedList<>());

    public void addSuccess(final CoordinationProtos.NodeEndpoint nodeEndpoint) {
      submissionSuccesses.add(nodeEndpoint);
    }

    public List<CoordinationProtos.NodeEndpoint> getSubmissionSuccesses() {
      return submissionSuccesses;
    }
  }
}
