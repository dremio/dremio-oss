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
package com.dremio.dac.service.credentials;

import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Provider;

/** A Grpc Credentials Service that delegates to {@link CredentialsService} */
public class CredentialsServiceGrpcImpl extends CredentialsServiceGrpc.CredentialsServiceImplBase {
  private final Provider<CredentialsService> credentialsService;

  @Inject
  public CredentialsServiceGrpcImpl(Provider<CredentialsService> credentialsService) {
    this.credentialsService = credentialsService;
  }

  @Override
  public void lookup(
      final LookupRequest request, final StreamObserver<LookupResponse> responseObserver) {
    try {
      final String secret = credentialsService.get().lookup(request.getPattern());
      responseObserver.onNext(LookupResponse.newBuilder().setSecret(secret).build());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
    } catch (CredentialsException e) {
      responseObserver.onError(Status.PERMISSION_DENIED.withCause(e).asRuntimeException());
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
    }
  }
}
