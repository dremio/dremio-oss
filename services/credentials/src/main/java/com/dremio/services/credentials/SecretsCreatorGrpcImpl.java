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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * A Grpc SecretsCreator Service that delegates to {@link SecretsCreatorImpl}. This runs on master
 * coordinator.
 */
public class SecretsCreatorGrpcImpl extends SecretsCreatorGrpc.SecretsCreatorImplBase {
  private final Provider<SecretsCreator> secretsCreatorProvider;

  @Inject
  public SecretsCreatorGrpcImpl(Provider<SecretsCreator> secretsCreatorProvider) {
    this.secretsCreatorProvider = secretsCreatorProvider;
  }

  @Override
  public void encrypt(EncryptRequest request, StreamObserver<EncryptResponse> responseObserver) {
    try {
      responseObserver.onNext(
          EncryptResponse.newBuilder()
              .setEncryptedSecret(
                  secretsCreatorProvider.get().encrypt(request.getSecret()).toString())
              .build());
      responseObserver.onCompleted();
    } catch (CredentialsException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }
}
