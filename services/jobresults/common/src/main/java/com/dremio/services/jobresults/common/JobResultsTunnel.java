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
package com.dremio.services.jobresults.common;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;

/**
 *
 */
@Options
public abstract class JobResultsTunnel implements AutoCloseable {
  public static final String AVOID_HEAP_COPY_IN_RESULTS_PATH_OPTION_NAME = "dremio.exec.avoid_heap_copy_in_results_path";

  // This option is used for all queries - UI, jdbc, etc.
  // 1. In executor while sending results from executor to coordinator
  // 2. In coordinator while receiving results from executor and also when forwarding results to other coordinator.
  // true means - a heap copy of results is avoided using custom grpc marshaller.
  // false means - a heap copy of results is made and default marshaller provided by grpc is used.
  public static final BooleanValidator AVOID_HEAP_COPY_IN_RESULTS_PATH =
    new BooleanValidator(AVOID_HEAP_COPY_IN_RESULTS_PATH_OPTION_NAME, false);

  public abstract  void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch data);

  @Override
  public void close() throws Exception {
  }
}
