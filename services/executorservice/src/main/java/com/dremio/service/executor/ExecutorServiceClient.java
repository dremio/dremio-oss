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
package com.dremio.service.executor;

import com.dremio.exec.proto.CoordExecRPC;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

/**
 * Client interface to executor service.
 */
public interface ExecutorServiceClient {

  public void startFragments(CoordExecRPC.InitializeFragments initializeFragments,
                             StreamObserver<Empty> responseObserver);

  public void activateFragments(CoordExecRPC.ActivateFragments activateFragments,
                                StreamObserver<Empty> responseObserver);

  public void cancelFragments(CoordExecRPC.CancelFragments cancelFragments,
                              StreamObserver<Empty> responseObserver);

  public void getNodeStats(Empty empty,
                           StreamObserver<CoordExecRPC.NodeStatResp> responseObserver);

  public void reconcileActiveQueries(CoordExecRPC.ActiveQueryList activeQueryList,
                                     StreamObserver<Empty> emptyStreamObserver);


}
