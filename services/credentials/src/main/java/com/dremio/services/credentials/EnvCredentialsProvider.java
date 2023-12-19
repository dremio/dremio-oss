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
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

/**
 * Env Credential Provider.
 */
@RemoteRestricted
public class EnvCredentialsProvider extends AbstractSimpleCredentialsProvider implements CredentialsProvider {
  private final Map<String, String> env;

  @Inject
  public EnvCredentialsProvider() {
    this(null);
  }

  @VisibleForTesting
  public EnvCredentialsProvider(Map<String, String> env) {
    super("env");
    this.env = env;
  }

  @Override
  protected String doLookup(URI uri) throws IllegalArgumentException {
    String key = uri.getSchemeSpecificPart();
    if (env == null) {
      return System.getenv().get(key);
    }
    return env.get(key);
  }

}
