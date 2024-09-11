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

import java.net.URI;
import java.util.Optional;
import javax.inject.Inject;

/**
 * A no-op SecretsCreator implementation. Useful for ensuring certain configurations of Dremio or
 * nodes don't encrypt.
 */
public class NoopSecretsCreator implements SecretsCreator {

  @Inject
  public NoopSecretsCreator() {}

  @Override
  public boolean isEncrypted(String secret) {
    throw new UnsupportedOperationException("isEncrypted not supported");
  }

  @Override
  public Optional<URI> encrypt(String secret) throws CredentialsException {
    throw new UnsupportedOperationException("encrypt not supported");
  }
}
