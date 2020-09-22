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

/**
 * Provider to {@code com.dremio.exec.proto.CoordinationProtos.NodeEndpoint}
 */
public interface ServiceSet extends ListenableSet {

  /**
   * Register an endpoint for the given service
   *
   * @param endpoint the endpoint to register
   * @return a handle to the registration
   * @throws NullPointerException if endpoint is {@code null}
   */
  RegistrationHandle register(NodeEndpoint endpoint);
}
