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
package com.dremio.exec.service.executor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.CatalogRPC;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.MemoryIterator;
import com.dremio.exec.store.sys.ThreadsIterator;
import com.dremio.exec.work.WorkStats;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.service.namespace.source.proto.SourceConfig;

import io.protostuff.ProtobufIOUtil;

public class ExecutorServiceImpl extends ExecutorService {
  private static final Logger logger = LoggerFactory.getLogger(ExecutorServiceImpl.class);

  private final FragmentExecutors fragmentExecutors;
  private final SabotContext context;
  private final FragmentExecutorBuilder builder;

  public ExecutorServiceImpl(FragmentExecutors fragmentExecutors, SabotContext context, FragmentExecutorBuilder builder) {
    this.fragmentExecutors = fragmentExecutors;
    this.context = context;
    this.builder = builder;
  }

  @Override
  public void startFragments(com.dremio.exec.proto.CoordExecRPC.InitializeFragments request,
                             io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    fragmentExecutors.startFragments(request, builder, responseObserver, context.getEndpoint());
  }

  @Override
  public void activateFragment(com.dremio.exec.proto.CoordExecRPC.ActivateFragments request,
                               io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    fragmentExecutors.activateFragments(request.getQueryId(), builder.getClerk());
    responseObserver.onCompleted();
  }

  @Override
  public void cancelFragments(com.dremio.exec.proto.CoordExecRPC.CancelFragments request,
                              io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    fragmentExecutors.cancelFragments(request.getQueryId(), builder.getClerk());
    responseObserver.onCompleted();
  }

  @Override
  public void reconcileActiveQueries(com.dremio.exec.proto.CoordExecRPC.ActiveQueryList request,
                                     io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    fragmentExecutors.reconcileActiveQueries(request, builder.getClerk());
    responseObserver.onCompleted();
  }

  @Override
  public void propagatePluginChange(com.dremio.exec.proto.CoordExecRPC.SourceWrapper request,
                                     io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    SourceConfig config = new SourceConfig();
    ProtobufIOUtil.mergeFrom(request.getBytes().toByteArray(), config, SourceConfig.getSchema());
    List<CoordinationProtos.NodeEndpoint> nodeEndpointList = request.getPluginChangeNodeEndpoints().getEndpointsIndexList();
    CatalogRPC.RpcType rpcType = CatalogRPC.RpcType.valueOf(request.getPluginChangeNodeEndpoints().getRpcType().name());

    logger.info("Propagate plugin change with rpc type {} to node endpoint list: {}", rpcType, nodeEndpointList);
    context.getCatalogService().communicateChangeToExecutors(nodeEndpointList, config, rpcType);
    responseObserver.onCompleted();
  }


  @Override
  public void getNodeStats(com.google.protobuf.Empty request,
                           io.grpc.stub.StreamObserver<com.dremio.exec.proto.CoordExecRPC.NodeStatResp> responseObserver) {
    try {
      CoordExecRPC.NodeStatResp resp = CoordExecRPC.NodeStatResp.newBuilder()
              .setNodeStats(getNodeStats())
              .setEndpoint(context.getEndpoint()).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private CoordExecRPC.NodeStats getNodeStats() {
    return getNodeStatsFromContext(context);
  }

  public static CoordExecRPC.NodeStats getNodeStatsFromContext(SabotContext context) {
    final ThreadsIterator threads = new ThreadsIterator(context, null);
    final MemoryIterator memoryIterator = new MemoryIterator(context, null);
    final WorkStats stats = context.getWorkStatsProvider().get();
    final CoordinationProtos.NodeEndpoint ep = context.getEndpoint();
    final double load = stats.getClusterLoad();
    final int configured_max_width = (int)context.getClusterResourceInformation().getAverageExecutorCores(context.getOptionManager());
    final int actual_max_width = (int) Math.max(1, configured_max_width * stats.getMaxWidthFactor());

    double memory = 0;
    double cpu = 0;

    // get cpu
    while(threads.hasNext()) {
      ThreadsIterator.ThreadSummary summary = (ThreadsIterator.ThreadSummary) threads.next();
      double cpuTime = summary.cpu_time == null ? 0 : summary.cpu_time;
      double numCores = summary.cores;
      cpu += (cpuTime / numCores);
    }

    // get memory
    if(memoryIterator.hasNext()) {
      MemoryIterator.MemoryInfo memoryInfo = ((MemoryIterator.MemoryInfo) memoryIterator.next());
      memory = memoryInfo.direct_current * 100.0 / memoryInfo.direct_max;
    }

    String ip =  null;
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      // no op
    }

    return CoordExecRPC.NodeStats.newBuilder()
            .setCpu(cpu)
            .setMemory(memory)
            .setVersion(DremioVersionInfo.getVersion())
            .setPort(ep.getFabricPort())
            .setName(ep.getAddress())
            .setIp(ip)
            .setStatus("green")
            .setLoad(load)
            .setConfiguredMaxWidth(configured_max_width)
            .setActualMaxWith(actual_max_width)
            .setCurrent(false)
            .build();
  }

  /**
   * Handler class to reject calls in two cases
   * 1. till the rpc layer is initialized.
   * 2. in coordinator since protocol is the same
   */
  public static final class NoExecutorService extends ExecutorService {

    public void startFragments(com.dremio.exec.proto.CoordExecRPC.InitializeFragments request,
                               io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
              "operations."));
    }

    public void activateFragment(com.dremio.exec.proto.CoordExecRPC.ActivateFragments request,
                                 io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
              "operations."));
    }

    public void cancelFragments(com.dremio.exec.proto.CoordExecRPC.CancelFragments request,
                                io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
              "operations."));
    }

    @Override
    public void reconcileActiveQueries(com.dremio.exec.proto.CoordExecRPC.ActiveQueryList request,
                                       io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
              "operations."));
    }

    @Override
    public void propagatePluginChange(com.dremio.exec.proto.CoordExecRPC.SourceWrapper request,
                                       io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
        "operations."));
    }

    public void getNodeStats(com.google.protobuf.Empty request,
                             io.grpc.stub.StreamObserver<com.dremio.exec.proto.CoordExecRPC.NodeStatResp> responseObserver) {
      responseObserver.onError(new RpcException("This daemon doesn't support execution " +
              "operations."));
    }
  }
}
