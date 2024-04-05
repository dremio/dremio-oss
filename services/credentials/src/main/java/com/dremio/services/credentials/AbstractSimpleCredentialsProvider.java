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

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Objects;

/** Abstract base class for credentials providers which only support a single scheme */
public abstract class AbstractSimpleCredentialsProvider implements CredentialsProvider {

  private final String scheme;

  protected AbstractSimpleCredentialsProvider(String scheme) {
    this.scheme = Objects.requireNonNull(scheme);
  }

  @Override
  public boolean isSupported(URI uri) {
    String otherScheme = uri.getScheme();
    return scheme.equalsIgnoreCase(otherScheme);
  }

  @Override
  public final String lookup(URI uri) throws CredentialsException {
    Preconditions.checkArgument(
        isSupported(uri), "Invalid URI type %s", uri.getScheme()); // Do not disclose the whole URI
    return doLookup(uri);
  }

  protected abstract String doLookup(URI uri) throws CredentialsException;
}
