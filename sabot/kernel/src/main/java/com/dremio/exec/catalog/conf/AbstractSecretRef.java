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

import static com.dremio.exec.catalog.conf.ConnectionConf.USE_EXISTING_SECRET_VALUE;

import com.dremio.service.namespace.SupportsDecoratingSecrets;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import io.protostuff.runtime.RuntimeEnv;
import java.net.URI;

/**
 * A Wrapper class for secret values to mask raw secret values. This has custom ser/de logic defined
 * by {@link SecretRefImplDelegate}. This should only be used to wrap raw secrets, not for secret
 * uris.
 */
public abstract class AbstractSecretRef
    implements SecretRef, SupportsDecoratingSecrets, RepresentableByURI {

  /**
   * Register SecretRef delegates here since Protostuff is configured statically. This should be
   * sufficient in most scenarios, but will not execute in time for upgrade scenarios, so we have
   * (usually) redundant registration in during ConnectionReader creation. Registration needs to
   * occur before ConnectionsSchemas are instantiated by the ConnectionReader.
   */
  static {
    registerDelegates();
  }

  public static void registerDelegates() {
    SecretRefImplDelegate.register(RuntimeEnv.ID_STRATEGY);
    SecretRefUnsafeDelegate.register(RuntimeEnv.ID_STRATEGY);
  }

  public AbstractSecretRef(String secret) {
    this.secret = secret;
  }

  private transient CredentialsService credentialsService;

  @Tag(1)
  protected String secret;

  /** Get the secret directly without resolution. Usages of this should be exceptional. */
  protected String getRaw() {
    return secret;
  }

  @Override
  public AbstractSecretRef decorateSecrets(CredentialsService credentialsService) {
    this.credentialsService = credentialsService;
    return this;
  }

  protected CredentialsService getCredentialsService() {
    return credentialsService;
  }

  @Override
  public boolean equals(Object obj) {
    if (SecretRef.EMPTY.equals(obj)) {
      return getRaw().isEmpty();
    } else if (SecretRef.EXISTING_VALUE.equals(obj)) {
      return getRaw().equals(USE_EXISTING_SECRET_VALUE);
    }
    if (obj instanceof AbstractSecretRef) {
      return getRaw().equals(((AbstractSecretRef) obj).getRaw());
    }
    return false;
  }

  @Override
  public URI getURI() {
    if (Strings.isNullOrEmpty(getRaw())) {
      return null;
    }
    try {
      final URI uri = CredentialsServiceUtils.safeURICreate(getRaw());
      // No scheme, means plaintext
      if (Strings.isNullOrEmpty(uri.getScheme())) {
        return null;
      }
      // If the URI is not recognized by the credentials service, treat as plaintext
      // null credentials service implies incoming secret from UI
      if (getCredentialsService() == null || !getCredentialsService().isSupported(uri)) {
        return null;
      }
      return uri;
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  @Override
  public int hashCode() {
    return getRaw().hashCode();
  }
}
