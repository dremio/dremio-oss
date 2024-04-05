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
package com.dremio.services.credentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.net.URI;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for the client {@link RemoteSecretsCreatorImpl} making RPC to the service {@link
 * SecretsCreatorGrpcImpl}.
 */
public class TestSecretsCreatorRPC {
  @ClassRule public static final GrpcCleanupRule GRPC_CLEANUP_RULE = new GrpcCleanupRule();
  public static final SecretsCreator mockSecretsCreator = mock(SecretsCreator.class);
  private static final String SECRET = "secret";
  private static final String ENCRYPTED_SECRET = "system:encryptedSecret";
  private static final String INVALID_SECRET_INPUT = "invalidSecretInput";
  private static RemoteSecretsCreatorImpl client;

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    // prepare service
    final SecretsCreatorGrpcImpl service = new SecretsCreatorGrpcImpl(() -> mockSecretsCreator);

    // prepare a server
    final String serverName = InProcessServerBuilder.generateName();
    GRPC_CLEANUP_RULE.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start());
    ManagedChannel channel =
        GRPC_CLEANUP_RULE.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());

    // create a client
    client = new RemoteSecretsCreatorImpl(() -> channel);
  }

  @Before
  public void beforeEachTest() throws Exception {
    reset(mockSecretsCreator);
    when(mockSecretsCreator.encrypt(eq(SECRET))).thenReturn(new URI(ENCRYPTED_SECRET));
  }

  @Test
  public void encrypt() throws CredentialsException {
    final URI result = client.encrypt(SECRET);
    assertEquals(ENCRYPTED_SECRET, result.toString());
  }

  @Test
  public void error() throws CredentialsException {
    when(mockSecretsCreator.encrypt(eq(INVALID_SECRET_INPUT)))
        .thenThrow(new CredentialsException("some error"));
    try {
      client.encrypt(INVALID_SECRET_INPUT);
      fail("This should fail with CredentialsException");
    } catch (CredentialsException e) {
      assertEquals(
          "Secondary coordinator cannot encode secret into an URI. Description: INVALID_ARGUMENT: some error",
          e.getMessage());
    } catch (Exception e) {
      fail("This shouldn't fail with a generic Exception");
    }
  }
}
