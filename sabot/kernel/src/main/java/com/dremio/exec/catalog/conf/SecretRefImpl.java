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
package com.dremio.exec.catalog.conf;

import com.dremio.services.credentials.CredentialsException;

/**
 * The primary, default SecretRef. Custom Protostuff ser/de logic is provided by {@link
 * SecretRefImplDelegate}. Secrets retrieved from SecretRefProto must be resolved with
 * CredentialsService.
 */
public class SecretRefImpl extends AbstractSecretRef {

  public SecretRefImpl(String secret) {
    super(secret);
  }

  @Override
  public String get() {
    if (getCredentialsService() == null) {
      throw new IllegalArgumentException("CredentialsService not available to lookup secret");
    }
    try {
      return getCredentialsService().lookup(secret);
    } catch (CredentialsException e) {
      throw new RuntimeException(e);
    }
  }
}
