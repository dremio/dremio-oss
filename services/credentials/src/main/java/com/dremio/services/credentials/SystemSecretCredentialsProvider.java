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

import com.google.inject.Inject;
import com.google.inject.Provider;
import java.net.URI;

/**
 * A credentials provider for System Secrets exposing system secret decryption allowing encrypted
 * secrets to be decrypted on lookup.
 */
public class SystemSecretCredentialsProvider extends AbstractSimpleCredentialsProvider
    implements CredentialsProvider {

  public static final String SECRET_PROVIDER_SCHEME = "system";

  private final Provider<Cipher> systemCipher;
  private final CredentialsProviderContext context;

  @Inject
  public SystemSecretCredentialsProvider(
      Provider<Cipher> systemCipher, CredentialsProviderContext context) {
    super(SECRET_PROVIDER_SCHEME);
    this.systemCipher = systemCipher;
    this.context = context;
  }

  @Override
  protected String doLookup(URI uri) throws CredentialsException {
    if (!context.isLeader()) { // always go to leader, do not depend on isRemoteEnabled
      return context.lookupOnLeader(uri.toString());
    } else {
      return systemCipher.get().decrypt(uri.getSchemeSpecificPart());
    }
  }
}
