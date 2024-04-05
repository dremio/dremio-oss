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
package com.dremio.exec.physical.base;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.record.BatchSchema;
import java.util.List;

public abstract class AbstractSender extends AbstractSingle implements Sender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSender.class);

  protected final BatchSchema schema;
  protected final int receiverMajorFragmentId;

  public AbstractSender(
      OpProps props, BatchSchema schema, PhysicalOperator child, int receiverMajorFragmentId) {
    super(props, child);
    this.schema = schema;
    this.receiverMajorFragmentId = receiverMajorFragmentId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitSender(this, value);
  }

  @Override
  public int getReceiverMajorFragmentId() {
    return receiverMajorFragmentId;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  /**
   * Resolve the index values and return the minor fragment endpoints.
   *
   * @param endpointsIndex index of endpoints.
   * @return List of MinorFragmentEndpoints each containing a minor fragment id and an endpoint.
   */
  @Override
  public List<MinorFragmentEndpoint> getDestinations(EndpointsIndex endpointsIndex) {
    return endpointsIndex.getFragmentEndpoints(getDestinations());
  }
}
