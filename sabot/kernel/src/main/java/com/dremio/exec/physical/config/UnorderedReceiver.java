/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;

import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.base.AbstractReceiver;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("unordered-receiver")
public class UnorderedReceiver extends AbstractReceiver {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnorderedReceiver.class);

  @JsonCreator
  public UnorderedReceiver(@JsonProperty("sender-major-fragment") int oppositeMajorFragmentId,
                           @JsonProperty("senders") List<MinorFragmentEndpoint> senders,
                           @JsonProperty("spooling") boolean spooling,
                           @JsonProperty("schema") BatchSchema schema) {
    super(oppositeMajorFragmentId, senders, spooling, schema);
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return true;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnorderedReceiver(this, value);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.UNORDERED_RECEIVER_VALUE;
  }
}
