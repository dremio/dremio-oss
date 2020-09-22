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

import java.util.Collection;

import javax.inject.Provider;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.service.jobresults.JobResultsResponse;
import com.dremio.service.jobresults.JobResultsServiceGrpc;
import com.dremio.service.maestroservice.MaestroServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * checks and forwards the request to target maestro server
 */
public class MaestroForwarderImpl implements MaestroForwarder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaestroForwarderImpl.class);

  private final Provider<ConduitProvider> conduitProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpointProvider;
  private final Provider<Collection<CoordinationProtos.NodeEndpoint>> jobServiceInstances;

  public MaestroForwarderImpl(
    final Provider<ConduitProvider> conduitProvider,
    final Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider,
    final Provider<Collection<CoordinationProtos.NodeEndpoint>> jobServiceInstances) {

    this.conduitProvider = conduitProvider;
    this.selfEndpointProvider = nodeEndpointProvider;
    this.jobServiceInstances = jobServiceInstances;
  }

  public void screenCompleted(NodeQueryScreenCompletion completion) {
    if (mustForwardRequest(completion.getForeman())) {
      logger.debug("Forwarding NodeQueryScreenCompletion request for Query {} from {} to target {}",
        QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint().getAddress(), completion.getForeman().getAddress());

      final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(completion.getForeman());
      final MaestroServiceGrpc.MaestroServiceBlockingStub stub = MaestroServiceGrpc.newBlockingStub(channel);
      stub.screenComplete(completion);
    } else {
      logger.debug("screen completion message arrived post query termination, dropping. Query [{}] from node {}.",
        QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
    }
  }

  public void nodeQueryCompleted(NodeQueryCompletion completion) {
    if (mustForwardRequest(completion.getForeman())) {
      logger.debug("Forwarding NodeQueryCompletion request for Query {} from {} to target {}",
        QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint().getAddress(), completion.getForeman().getAddress());

      final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(completion.getForeman());
      final MaestroServiceGrpc.MaestroServiceBlockingStub stub = MaestroServiceGrpc.newBlockingStub(channel);
      stub.nodeQueryComplete(completion);
    } else {
      logger.debug("A node query completion message arrived post query termination, dropping. Query [{}] from node {}.",
        QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
    }
  }

  public void nodeQueryMarkFirstError(NodeQueryFirstError error) {
    if (mustForwardRequest(error.getForeman())) {
      logger.debug("Forwarding NodeQueryFirstError request for Query {} from {} to target {}",
        QueryIdHelper.getQueryId(error.getHandle().getQueryId()), error.getEndpoint().getAddress(), error.getForeman().getAddress());

      final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(error.getForeman());
      final MaestroServiceGrpc.MaestroServiceBlockingStub stub = MaestroServiceGrpc.newBlockingStub(channel);
      stub.nodeFirstError(error);
    } else {
      logger.debug("A node query error message arrived post query termination, dropping. Query [{}] from node {}.",
        QueryIdHelper.getQueryId(error.getHandle().getQueryId()), error.getEndpoint());
    }
  }

  public void dataArrived(JobResultsRequest jobResultsRequest, ResponseSender sender) {
    if (mustForwardRequest(jobResultsRequest.getForeman())) {
      logger.debug("Forwarding DataArrived request for Query {} to target {}",
        QueryIdHelper.getQueryId(jobResultsRequest.getHeader().getQueryId()), jobResultsRequest.getForeman().getAddress());

      final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(jobResultsRequest.getForeman());
      final JobResultsServiceGrpc.JobResultsServiceStub stub = JobResultsServiceGrpc.newStub(channel);
      StreamObserver<JobResultsRequest> streamObserver = stub.jobResults(new StreamObserver<JobResultsResponse>() {
        @Override
        public void onNext(JobResultsResponse value) { ackit(sender); }

        @Override
        public void onError(Throwable t) { ackit(sender); }

        @Override
        public void onCompleted() { ackit(sender); }
      });

      streamObserver.onNext(jobResultsRequest);

    } else {
      logger.debug("User data arrived post query termination, dropping. Data was from QueryId: {}.",
        QueryIdHelper.getQueryId(jobResultsRequest.getHeader().getQueryId()));
    }
  }

  private void ackit(ResponseSender sender){
    sender.send(new Response(RpcType.ACK, Acks.OK));
  }

  private boolean mustForwardRequest(final CoordinationProtos.NodeEndpoint endpoint) {
    if (endpoint == null) { //for UTs
      return false;
    }
    return !endpoint.equals(selfEndpointProvider.get())
      && jobServiceInstances.get().contains(endpoint);
  }
}
