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

package com.dremio.service.conduit.server;

import com.dremio.service.grpc.CloseableBindableService;

import io.grpc.BindableService;
import io.grpc.HandlerRegistry;
import io.grpc.ServerServiceDefinition;


/**
 * {@link BindableService Bindable services} that register with this registry will be added to the
 * handler registry during {@link ConduitServer} startup.
 */
public interface ConduitServiceRegistry {

  /**
   * Register the service.
   *
   * @param bindableService bindable service
   */
  void registerService(BindableService bindableService);

  /**
   * Register a service that also Autocloseable
   * @param bindableService
   */
  void registerService(CloseableBindableService bindableService);

  /**
   * Register the service.
   *
   * @param serverServiceDefinition server service
   */
  void registerServerService(ServerServiceDefinition serverServiceDefinition);

  /**
   * Used for proxying. Only one fallback handler can be used.
   */
  void registerFallbackHandler(HandlerRegistry handlerRegistry);
}
