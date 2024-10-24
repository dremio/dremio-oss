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
package com.dremio.exec.work.protector;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.rpc.BaseRpcOutcomeListener;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.telemetry.api.metrics.SimpleTimedHistogram;
import io.micrometer.core.instrument.Timer.ResourceSample;
import io.netty.buffer.ByteBuf;

/***
 * Decorator which helps provide instrumentation coverage to the base response handler. This favours
 * composition over inheritance, thereby avoiding overburdening the wrapped class with instrumentation code.
 */
public class InstrumentedUserResponseHandler implements CompletedUserResponseHandler {
  private final CompletedUserResponseHandler baseUserResponseHandler;
  private int batchCount;
  private final SimpleTimedHistogram rpcLatencyTimer;
  private static final String METRIC_TAG_KEY_RPC_TYPE = "rpc.type";
  private static final String METRIC_TAG_KEY_BATCH = "batch";
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(InstrumentedUserResponseHandler.class);

  public InstrumentedUserResponseHandler(CompletedUserResponseHandler baseUserResponseHandler) {
    this.baseUserResponseHandler = baseUserResponseHandler;
    this.batchCount = 0;
    this.rpcLatencyTimer =
        SimpleTimedHistogram.of(
            "coordinator.userRpc.time", "measures latency for coordinator user rpcs");
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    batchCount++;
    logger.debug(
        "Sending query data batch {} with row count {} for query id {}",
        batchCount,
        result.getHeader().getRowCount(),
        QueryIdHelper.getQueryId(result.getHeader().getQueryId()));
    ResourceSample sample = rpcLatencyTimer.start();
    InstrumentedRpcOutcomeListener instrumentedRpcOutcomeListener =
        new InstrumentedRpcOutcomeListener(
            outcomeListener,
            () -> {
              sample
                  .tags(
                      METRIC_TAG_KEY_RPC_TYPE,
                      RpcType.QUERY_DATA.toString(),
                      METRIC_TAG_KEY_BATCH,
                      String.valueOf(batchCount))
                  .close();
              logger.debug(
                  "Query data batch {} with row count {} for query id {} sent",
                  batchCount,
                  result.getHeader().getRowCount(),
                  QueryIdHelper.getQueryId(result.getHeader().getQueryId()));
            });
    baseUserResponseHandler.sendData(instrumentedRpcOutcomeListener, result);
  }

  @Override
  public void completed(UserResult result) {
    this.completed(result, new BaseRpcOutcomeListener<Ack>());
  }

  @Override
  public void completed(UserResult result, RpcOutcomeListener<Ack> outcomeListener) {
    ResourceSample sample = rpcLatencyTimer.start();
    logger.debug(
        "Sending terminal query results message for query id {}. Total batches sent: {}",
        QueryIdHelper.getQueryId(result.getProfile().getId()),
        batchCount);
    InstrumentedRpcOutcomeListener instrumentedRpcOutcomeListener =
        new InstrumentedRpcOutcomeListener(
            outcomeListener,
            () -> {
              sample.tags(METRIC_TAG_KEY_RPC_TYPE, RpcType.QUERY_RESULT.toString()).close();
              logger.debug(
                  "Terminal query results message sent for query id {}. Total batches sent: {}",
                  QueryIdHelper.getQueryId(result.getProfile().getId()),
                  batchCount);
            });
    baseUserResponseHandler.completed(result, instrumentedRpcOutcomeListener);
  }

  public static class InstrumentedRpcOutcomeListener implements RpcOutcomeListener<Ack> {

    private final RpcOutcomeListener<Ack> innerRpcOutcomeListener;
    private final Runnable measuringFunction;

    private InstrumentedRpcOutcomeListener(
        RpcOutcomeListener<Ack> outcomeListener, Runnable measuringFunction) {
      this.innerRpcOutcomeListener = outcomeListener;
      this.measuringFunction = measuringFunction;
    }

    @Override
    public void dataOnWireCallback() {
      innerRpcOutcomeListener.dataOnWireCallback();
      measuringFunction.run();
    }

    @Override
    public void failed(RpcException ex) {
      innerRpcOutcomeListener.failed(ex);
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      innerRpcOutcomeListener.success(value, buffer);
    }

    @Override
    public void interrupted(InterruptedException e) {
      innerRpcOutcomeListener.interrupted(e);
    }
  }
}
