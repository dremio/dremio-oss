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

import javax.inject.Provider;

import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import com.google.inject.Inject;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * A Grpc Credentials Service that delegates to {@link CredentialsService}
 */
public class CredentialsServiceImpl extends CredentialsServiceGrpc.CredentialsServiceImplBase {

  private final Provider<CredentialsService> credentialsService;

  @Inject
  public CredentialsServiceImpl(Provider<CredentialsService> credentialsService) {
    this.credentialsService = credentialsService;
  }

  @Override
  public void lookup(final LookupRequest request, final StreamObserver<LookupResponse> responseObserver) {
    try {
      final String secret = credentialsService.get().lookup(request.getPattern());
      responseObserver.onNext(LookupResponse.newBuilder()
        .setSecret(secret)
        .build());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
    } catch (/* CredentialsException | */ Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
    }
  }
}
