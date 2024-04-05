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
import javax.inject.Inject;

/** Test only Credentials Provider TODO: elaborate */
class TestingCredentialsProvider implements CredentialsProvider {
  private static final String RESOLVED_TAG = "resolvedByTestingCredentialsProvider:";
  static final String SCHEME = "testing-credentials-provider";
  private final CredentialsProviderContext context;

  static boolean isResolved(String resolvedSecret) {
    return resolvedSecret.contains(RESOLVED_TAG);
  }

  @Inject
  public TestingCredentialsProvider(CredentialsProviderContext context) {
    this.context = context;
  }

  @Override
  public String lookup(URI pattern) throws IllegalArgumentException, CredentialsException {
    if (!context.isLeader() && context.isRemoteEnabled()) {
      return context.lookupOnLeader(pattern.toString());
    } else {
      return RESOLVED_TAG + pattern;
    }
  }

  @Override
  public boolean isSupported(URI pattern) {
    return pattern.getScheme().equals(SCHEME);
  }
}
