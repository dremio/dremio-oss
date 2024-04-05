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

import javax.inject.Inject;

/**
 * A no-op context implementation. Useful for leader only setups such as tests or admin commands.
 */
class NoopCredentialsProviderContext implements CredentialsProviderContext {
  @Inject
  NoopCredentialsProviderContext() {}

  @Override
  public boolean isLeader() {
    return true;
  }

  @Override
  public boolean isRemoteEnabled() {
    return false;
  }

  @Override
  public String lookupOnLeader(String uri) throws CredentialsException {
    throw new UnsupportedOperationException("Not configured to perform remote credentials lookup");
  }
}
