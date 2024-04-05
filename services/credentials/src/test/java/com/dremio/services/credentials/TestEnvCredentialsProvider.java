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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Tests for lookup in Env Credential Provider. */
public class TestEnvCredentialsProvider {

  @Test
  public void testProviderEnvLookup() throws Exception {

    String originalString = "ldapPasswordCipherValidation";
    Map<String, String> env = new HashMap<String, String>();
    // set env $ldap
    String ldapEnv = "ldap";
    env.put(ldapEnv, originalString);

    final String bindPassword = "env:".concat(ldapEnv);

    URI uri = URI.create(bindPassword);
    EnvCredentialsProvider provider = new EnvCredentialsProvider(env);

    boolean supported = provider.isSupported(uri);
    assertTrue(supported);

    String ldap = provider.lookup(uri);
    assertEquals(originalString, ldap);
  }
}
