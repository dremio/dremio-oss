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

import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc.CredentialsServiceBlockingStub;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;

import io.grpc.StatusRuntimeException;

/**
 * Credentials Service for executors
 */
@Options
public class ExecutorCredentialsService implements CredentialsService {
  public static final TypeValidators.BooleanValidator REMOTE_LOOKUP_ENABLED
    = new TypeValidators.BooleanValidator("services.credentials.exec.remote_lookup.enabled", false);

  private final CredentialsService delegate;
  private final Provider<CredentialsServiceBlockingStub> credentialsServiceBlockingStub;
  private final Provider<OptionManager> optionManager;

  public ExecutorCredentialsService(
    CredentialsService delegate,
    Provider<CredentialsServiceBlockingStub> credentialsServiceBlockingStub,
    Provider<OptionManager> optionManager
  ) {
    this.credentialsServiceBlockingStub = credentialsServiceBlockingStub;
    this.delegate = delegate;
    this.optionManager = optionManager;
  }

  private boolean isRemoteLookupEnabled() {
    return optionManager.get().getOption(REMOTE_LOOKUP_ENABLED);
  }

  @Override
  public String lookup(final String pattern) throws IllegalArgumentException, CredentialsException {
    // perform remote lookup if it is necessary, otherwise default to normal lookup
    if (isRemoteLookupEnabled()) {
      return remoteLookup(pattern);
    } else {
      return delegate.lookup(pattern);
    }
  }

  private String remoteLookup(final String pattern) throws IllegalArgumentException, CredentialsException {
    try {
      final LookupRequest lookupRequest = LookupRequest.newBuilder()
        .setPattern(pattern)
        .build();
      final LookupResponse lookupResponse = credentialsServiceBlockingStub.get().lookup(lookupRequest);
      return lookupResponse.getSecret();
    } catch (StatusRuntimeException sre) {
      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(sre);
        case INTERNAL:
          throw new CredentialsException(sre.getMessage(), sre);
        default:
          throw new RuntimeException(sre);
      }
    }
  }

  @Override
  public void start() throws Exception {
    delegate.start();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
