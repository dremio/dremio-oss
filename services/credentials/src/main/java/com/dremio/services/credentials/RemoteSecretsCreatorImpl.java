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

import com.dremio.services.credentials.proto.SecretsCreatorGrpc;
import com.dremio.services.credentials.proto.SecretsCreatorRPC.EncryptRequest;
import com.dremio.services.credentials.proto.SecretsCreatorRPC.EncryptResponse;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Used by the secondary coordinators make RPC to master coordinator to encrypt a secret in-line.
 */
public class RemoteSecretsCreatorImpl implements SecretsCreator {

  private final Provider<ManagedChannel> channelToMaster;

  @Inject
  public RemoteSecretsCreatorImpl(Provider<ManagedChannel> channelToMaster) {
    this.channelToMaster = channelToMaster;
  }

  @Override
  public boolean isEncrypted(String secret) {
    throw new UnsupportedOperationException("isEncrypted is not supported on non-leader nodes.");
  }

  /**
   * Encrypt the given secret in-line by making RPC to master coordinator, which will return the
   * encrypted value as a CredentialsService URI.
   *
   * @param secret secret in plain-text
   * @return an encrypted secret URI using system cipher
   */
  @Override
  public URI encrypt(String secret) throws CredentialsException {
    final EncryptResponse encryptedSecret;
    try {
      encryptedSecret =
          SecretsCreatorGrpc.newBlockingStub(channelToMaster.get())
              .encrypt(EncryptRequest.newBuilder().setSecret(secret).build());
    } catch (StatusRuntimeException sre) {
      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          throw new CredentialsException(
              String.format(
                  "Secondary coordinator cannot encode secret into an URI. Description: %s",
                  sre.getMessage()));
        case INTERNAL:
          throw new CredentialsException(
              String.format(
                  "Secondary coordinator failed to encode secret into an URI. Description: %s",
                  sre.getMessage()));
        default:
          throw new RuntimeException(
              String.format(
                  "Secondary coordinator failed to encode secret into an URI. Description: %s",
                  sre.getMessage()));
      }
    }
    try {
      return new URI(encryptedSecret.getEncryptedSecret());
    } catch (URISyntaxException e) {
      throw new CredentialsException("Secondary coordinator received an invalid secret URI");
    }
  }
}
