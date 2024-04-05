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

/** Exposes remote lookup context to a {@link CredentialsProvider}. */
public interface CredentialsProviderContext {

  /**
   * Returns true on leader instance (on master coordinator).
   *
   * <p>Based on configuration or implementation spec, a {@link CredentialsProvider} may choose to
   * resolve a secret only on the leader. Based on the return value, the provider may initialize its
   * internal resources differently while starting. Also, if this method returns false, the provider
   * could use {@link #lookupOnLeader(String)} to make an RPC to the leader which looks up the
   * value.
   *
   * @return true iff the node is the leader
   */
  boolean isLeader();

  /**
   * Returns true when remote lookups are enabled via option/support key REMOTE_LOOKUP_ENABLED
   *
   * @return true if remote lookups are enabled
   */
  boolean isRemoteEnabled();

  /**
   * Make an RPC to the leader to lookup the uri.
   *
   * @param uri secret uri
   * @return secret value
   * @throws CredentialsException on error
   */
  String lookupOnLeader(String uri) throws CredentialsException;
}
