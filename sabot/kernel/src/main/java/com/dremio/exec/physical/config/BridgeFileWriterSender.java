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

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.physical.base.AbstractSender;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Collections;
import java.util.List;

/** POP for a sender that writes to local files instead of a socket. */
@JsonTypeName("bridge-file-writer-sender")
public class BridgeFileWriterSender extends AbstractSender {
  private String bridgeSetId;

  @JsonCreator
  public BridgeFileWriterSender(
      @JsonProperty("props") OpProps props,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("bridgeSetId") String bridgeSetId) {
    super(props, schema, child, -1);
    this.bridgeSetId = bridgeSetId;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new BridgeFileWriterSender(props, schema, child, bridgeSetId);
  }

  @Override
  public List<CoordExecRPC.MinorFragmentIndexEndpoint> getDestinations() {
    // since the sender writes to a file, the list of destinations is not relevant.
    return Collections.emptyList();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.BRIDGE_FILE_WRITER_SENDER_VALUE;
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  public static String computeUniqueId(
      UserBitShared.QueryId queryId, String bridgeSetId, int minorFragmentId) {
    return String.format(
        "bridge_%s_%s_%s", QueryIdHelper.getQueryId(queryId), bridgeSetId, minorFragmentId);
  }
}
