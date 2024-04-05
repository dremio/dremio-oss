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
package com.dremio.service.maestroservice;

import com.dremio.exec.proto.CoordExecRPC;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

/**
 * Maestro client interface for executors to interact with it. Will have a product specific (fabric
 * based) and daas specific implementations.
 */
public interface MaestroClient {
  /**
   * Used to inform maestro of screen completion.
   *
   * @param request
   * @param responseObserver
   * @return
   */
  public void screenComplete(
      CoordExecRPC.NodeQueryScreenCompletion request, StreamObserver<Empty> responseObserver);

  /**
   * Used to inform maestro of query completion on a specific node.
   *
   * @param request
   * @param responseObserver
   * @return
   */
  public void nodeQueryComplete(
      CoordExecRPC.NodeQueryCompletion request, StreamObserver<Empty> responseObserver);

  /**
   * Used to inform maestro of an error processing a query on a specific node.
   *
   * @param request
   * @param responseObserver
   * @return
   */
  public void nodeFirstError(
      CoordExecRPC.NodeQueryFirstError request, StreamObserver<Empty> responseObserver);
}
