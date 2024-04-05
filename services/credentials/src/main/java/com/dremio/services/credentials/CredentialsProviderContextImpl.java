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

import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.function.BooleanSupplier;
import javax.inject.Provider;

/** Injected class that exposes remote lookup context to a {@link CredentialsProvider}. */
public class CredentialsProviderContextImpl implements CredentialsProviderContext {
  private final BooleanSupplier isLeader;
  private final BooleanSupplier isRemoteEnabled;
  private final Provider<ManagedChannel> channelToLeader;

  @Inject
  CredentialsProviderContextImpl(
      @Leader BooleanSupplier isLeader,
      @RemoteEnabled BooleanSupplier isRemoteEnabled,
      @Named("channelToLeader") Provider<ManagedChannel> channelToLeader) {
    this.isLeader = isLeader;
    this.isRemoteEnabled = isRemoteEnabled;
    this.channelToLeader = channelToLeader;
  }

  @Override
  public boolean isLeader() {
    return isLeader.getAsBoolean();
  }

  @Override
  public boolean isRemoteEnabled() {
    return isRemoteEnabled.getAsBoolean();
  }

  @Override
  public String lookupOnLeader(String uri) throws CredentialsException {
    try {
      final LookupRequest lookupRequest = LookupRequest.newBuilder().setPattern(uri).build();
      return CredentialsServiceGrpc.newBlockingStub(channelToLeader.get())
          .lookup(lookupRequest)
          .getSecret();
    } catch (StatusRuntimeException sre) {
      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(sre);
        case PERMISSION_DENIED:
        case INTERNAL:
          throw new CredentialsException(sre.getMessage(), sre);
        default:
          throw new RuntimeException(sre);
      }
    }
  }
}
