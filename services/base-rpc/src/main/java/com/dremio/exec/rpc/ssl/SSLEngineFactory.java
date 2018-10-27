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
package com.dremio.exec.rpc.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import io.netty.buffer.ByteBufAllocator;

/**
 * Factory to create {@link SSLEngine}.
 */
public interface SSLEngineFactory {

  /**
   * Creates a {@link SSLEngine} to be used on server-side of SSL negotiation.
   *
   * @param allocator      allocator
   * @param peerHost       peer hostname
   * @param peerPort       peer port
   * @return server-side SSL engine
   * @throws SSLException if there are any errors creating the engine
   */
  SSLEngine newServerEngine(ByteBufAllocator allocator, String peerHost, int peerPort)
      throws SSLException;

  /**
   * Create a {@link SSLEngine} to be used on client-side of SSL negotiation.
   *
   * @param allocator allocator
   * @param peerHost  peer hostname
   * @param peerPort  peer port
   * @return client-side SSL engine
   * @throws SSLException if there are any errors creating the engine
   */
  SSLEngine newClientEngine(ByteBufAllocator allocator, String peerHost, int peerPort)
      throws SSLException;

}
