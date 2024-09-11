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
package com.dremio.service.users;

import com.dremio.authenticator.AuthException;
import com.dremio.authenticator.AuthRequest;
import com.dremio.authenticator.AuthResult;
import com.dremio.authenticator.Authenticator;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.jetbrains.annotations.NotNull;

/**
 * A Basic Authenticator that uses the LocalUsernamePasswordAuthProvider for username/password
 * authentication.
 */
@Singleton
public class BasicAuthenticator implements Authenticator {

  private final Provider<LocalUsernamePasswordAuthProvider> usernamePasswordAuthProviderProvider;

  private LocalUsernamePasswordAuthProvider usernamePasswordAuthProvider;

  @Inject
  public BasicAuthenticator(
      Provider<LocalUsernamePasswordAuthProvider> usernamePasswordAuthProviderProvider) {
    this.usernamePasswordAuthProviderProvider = usernamePasswordAuthProviderProvider;
  }

  @Override
  public void start() {
    usernamePasswordAuthProvider = usernamePasswordAuthProviderProvider.get();
  }

  /** Validates username/password. */
  @Override
  @NotNull
  public AuthResult authenticate(AuthRequest request) throws AuthException {
    return usernamePasswordAuthProvider.validate(request);
  }
}
