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
package com.dremio.service.coordinator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.Collection;

/** Helper to make sure constructor arguments bind correctly (TODO: remove this) */
public class ServiceSetDecorator implements ServiceSet {

  private final ServiceSet delegate;

  public ServiceSetDecorator(ServiceSet delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public RegistrationHandle register(NodeEndpoint endpoint) {
    return delegate.register(endpoint);
  }

  @Override
  public Collection<NodeEndpoint> getAvailableEndpoints() {
    return delegate.getAvailableEndpoints();
  }

  @Override
  public void addNodeStatusListener(NodeStatusListener listener) {
    delegate.addNodeStatusListener(listener);
  }

  @Override
  public void removeNodeStatusListener(NodeStatusListener listener) {
    delegate.removeNodeStatusListener(listener);
  }
}
