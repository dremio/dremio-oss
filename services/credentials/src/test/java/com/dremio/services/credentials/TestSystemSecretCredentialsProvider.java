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

import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Providers;
import java.net.URI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link SystemSecretCredentialsProvider}. */
public class TestSystemSecretCredentialsProvider extends DremioTest {
  private SystemCipher systemCipher;
  private CredentialsService credentialsService;

  // TODO: replace indirection with something else (i.e SecretsManager#encrypt) since SystemCipher
  // does not handle URI construction
  private URI encrypt(String pattern) throws Exception {
    return new URI(
        SystemSecretCredentialsProvider.SECRET_PROVIDER_SCHEME,
        systemCipher.encrypt(pattern),
        null);
  }

  @Before
  public void before() throws Exception {
    systemCipher = new SystemCipher(DEFAULT_DREMIO_CONFIG, credentialsService);
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(
                DEFAULT_DREMIO_CONFIG, ImmutableSet.of(SystemSecretCredentialsProvider.class)),
            new CipherModule(Providers.of(systemCipher)));
    credentialsService = injector.getInstance(CredentialsService.class);
  }

  @After
  public void after() throws Exception {
    credentialsService.close();
  }

  @Test
  public void testBasic() throws Exception {
    final String originalString = "secretEncryptDecryptValidation";
    final URI encrypted = encrypt(originalString);

    // This is just a placeholder until #encrypt is removed since schema is set to always be correct
    assertEquals(SystemSecretCredentialsProvider.SECRET_PROVIDER_SCHEME, encrypted.getScheme());

    final String decrypted = credentialsService.lookup(encrypted.toString());
    assertEquals(originalString, decrypted);
  }
}
