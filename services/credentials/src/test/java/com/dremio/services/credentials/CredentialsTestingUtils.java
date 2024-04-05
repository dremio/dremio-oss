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

import static com.dremio.test.DremioTest.DEFAULT_DREMIO_CONFIG;

import com.dremio.config.DremioConfig;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public final class CredentialsTestingUtils {
  private static final String REMOTE_RESOLVE_TAG = "REMOTE-RESOLVE:";

  public static boolean isRemoteResolved(String resolvedSecret) {
    return resolvedSecret.contains(REMOTE_RESOLVE_TAG);
  }

  public static DremioConfig getLeaderConfig() {
    return DEFAULT_DREMIO_CONFIG
        .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, true)
        .withValue(DremioConfig.ENABLE_MASTER_BOOL, true)
        .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, false);
  }

  public static DremioConfig getNonLeaderConfig() {
    return DEFAULT_DREMIO_CONFIG
        .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
        .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, true);
  }

  private CredentialsTestingUtils() {}

  public static class TestingCredentialsServiceGrpcServer
      extends CredentialsServiceGrpc.CredentialsServiceImplBase {
    private final CredentialsServiceImpl delegate;

    public TestingCredentialsServiceGrpcServer(CredentialsServiceImpl delegate) {
      this.delegate = delegate;
    }

    @Override
    public void lookup(
        final LookupRequest request, final StreamObserver<LookupResponse> responseObserver) {
      try {
        responseObserver.onNext(
            LookupResponse.newBuilder()
                .setSecret(REMOTE_RESOLVE_TAG + delegate.lookup(request.getPattern()))
                .build());
        responseObserver.onCompleted();
      } catch (Exception e) {
        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
      }
    }
  }
}
