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
package com.dremio.exec.work;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.BaseRpcOutcomeListener;

public abstract class EndpointListener<RET, V> extends BaseRpcOutcomeListener<RET> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointListener.class);

  protected final NodeEndpoint endpoint;
  protected final V value;

  public EndpointListener(NodeEndpoint endpoint, V value) {
    super();
    this.endpoint = endpoint;
    this.value = value;
  }

  protected NodeEndpoint getEndpoint() {
    return endpoint;
  }

  protected V getValue() {
    return value;
  }
}
