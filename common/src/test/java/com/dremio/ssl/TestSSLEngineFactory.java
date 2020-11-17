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
package com.dremio.ssl;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.junit.Test;

import io.netty.buffer.ByteBufAllocator;

/**
 * Test SSLEngineFactory
 */
public class TestSSLEngineFactory {

  @Test
  public void testServerNameIndication() throws SSLException {
    // Arrange
    final SSLConfig config = SSLConfig.newBuilder().build();
    final SSLEngineFactory factory = SSLEngineFactoryImpl.create(Optional.of(config)).get();
    final String host = "testhost";
    final SSLEngine clientEngine = factory.newClientEngine(ByteBufAllocator.DEFAULT, host, 100);
    final SSLParameters params = clientEngine.getSSLParameters();

    // Assert
    assertEquals(1, params.getServerNames().size());
    final SNIHostName hostSni = (SNIHostName) params.getServerNames().get(0);
    assertEquals(host, hostSni.getAsciiName());
  }
}
