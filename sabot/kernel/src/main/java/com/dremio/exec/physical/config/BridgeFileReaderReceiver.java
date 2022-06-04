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
package com.dremio.exec.physical.config;

import java.util.Collections;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractReceiver;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * POP for a receiver that reads from local files instead of a socket.
 */
@JsonTypeName("bridge-file-reader-receiver")
public class BridgeFileReaderReceiver extends AbstractReceiver {
  private final String bridgeSetId;

  @JsonCreator
  public BridgeFileReaderReceiver(
    @JsonProperty("props") OpProps props,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("senderMajorFragmentId") int senderMajorFragmentId,
    @JsonProperty("bridgeSetId") String bridgeSetId
  ) {
    super(props, schema, senderMajorFragmentId, false);
    this.bridgeSetId = bridgeSetId;
  }

  @Override
  public List<CoordExecRPC.MinorFragmentIndexEndpoint> getProvidingEndpoints() {
    // not relevant since this reads from a file.
    return Collections.emptyList();
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return false;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new BridgeFileReaderReceiver(props, getSchema(), getSenderMajorFragmentId(), bridgeSetId);
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.BRIDGE_FILE_READER_RECEIVER_VALUE;
  }
}
