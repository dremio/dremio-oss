/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;

import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A receiver is one half of an exchange operator. The receiver is responsible for taking in one or more streams from
 * corresponding Senders.  Receivers are a special type of Physical SqlOperatorImpl that are typically only expressed within the execution plan.
 */
public interface Receiver extends FragmentLeaf {

  /**
   * A receiver is expecting streams from one or more providing endpoints.
   * @return List of sender MinorFragmentEndpoints each containing sender fragment MinorFragmentId and endpoint where
   * it is running.
   */
  List<MinorFragmentEndpoint> getProvidingEndpoints();

  /**
   * Whether or not this receive supports out of order exchange. This provides a hint for the scheduling node on whether
   * the receiver can start work if only a subset of all sending endpoints are currently providing data. A random
   * receiver would supports this form of operation. A NWAY receiver would not.
   *
   * @return True if this receiver supports working on a streaming/out of order input.
   */
  @JsonIgnore
  boolean supportsOutOfOrderExchange();

  @JsonProperty("sender-major-fragment")
  int getOppositeMajorFragmentId();

  @JsonProperty("spooling")
  boolean isSpooling();
}
