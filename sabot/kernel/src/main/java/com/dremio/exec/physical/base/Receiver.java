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

import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * A receiver is one half of an exchange operator. The receiver is responsible for taking in one or
 * more streams from corresponding Senders. Receivers are a special type of Physical SqlOperatorImpl
 * that are typically only expressed within the execution plan.
 */
@Options
public interface Receiver extends FragmentLeaf {

  /**
   * A receiver is expecting streams from one or more providing endpoints.
   *
   * @return List of sender MinorFragmentIndexEndpoints each containing sender fragment
   *     MinorFragmentId and endpoint index where it is running.
   */
  List<MinorFragmentIndexEndpoint> getProvidingEndpoints();

  /**
   * Whether or not this receive supports out of order exchange. This provides a hint for the
   * scheduling node on whether the receiver can start work if only a subset of all sending
   * endpoints are currently providing data. A random receiver would supports this form of
   * operation. A NWAY receiver would not.
   *
   * @return True if this receiver supports working on a streaming/out of order input.
   */
  @JsonIgnore
  boolean supportsOutOfOrderExchange();

  int getSenderMajorFragmentId();

  BatchSchema getSchema();

  @JsonProperty("spooling")
  boolean isSpooling();
}
