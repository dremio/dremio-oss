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
package com.dremio.exec.physical.base;

import java.util.Collections;
import java.util.Iterator;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class AbstractReceiver extends AbstractBase implements Receiver {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractReceiver.class);

  private final BatchSchema schema;
  private final int senderMajorFragmentId;
  private final boolean spooling;

  /**
   *
   * @param props
   * @param schema
   * @param oppositeMajorFragmentId MajorFragmentId of fragments that are sending data to this receiver.
   * @param senders List of sender MinorFragmentEndpoints each containing sender MinorFragmentId and SabotNode endpoint
   *                where it is running.
   * @param spooling
   */
  public AbstractReceiver(
      OpProps props,
      BatchSchema schema,
      int senderMajorFragmentId,
      boolean spooling) {
    super(props);
    this.schema = schema;
    this.senderMajorFragmentId = senderMajorFragmentId;
    this.spooling = spooling;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitReceiver(this, value);
  }

  public int getSenderMajorFragmentId() {
    return senderMajorFragmentId;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @JsonIgnore
  public int getNumSenders() {
    return getProvidingEndpoints().size();
  }

  @Override
  public boolean isSpooling() {
    return spooling;
  }


}

