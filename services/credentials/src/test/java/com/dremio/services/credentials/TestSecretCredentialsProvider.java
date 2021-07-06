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

import static com.dremio.config.DremioConfig.CREDENTIALS_KEYSTORE_PASSWORD;
import static com.dremio.config.DremioConfig.LOCAL_WRITE_PATH_STRING;
import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for:
 * 1) Lookup in Secret Credential Provider with raw keystore password
 * 2) Lookup in Secret Credential Provider with data URI keystore password
 * 3) Lookup in Secret Credential Provider with env URI keystore password
 * 4) Lookup in Secret Credential Provider with file URI keystore password
 * 5) Lookup in Secret Credential Provider with secret URI keystore password
 */
public class TestSecretCredentialsProvider extends DremioTest {
  private static final Map<String, String> MOCK_ENVIRONMENT = new ConcurrentHashMap<>(System.getenv());

  private static class MockEnvCredentialsProvider extends EnvCredentialsProvider {
    @SuppressWarnings("unused")
    public MockEnvCredentialsProvider() {
      super(MOCK_ENVIRONMENT);
    }
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSecretProviderLookupWithDefaultPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";
    DremioConfig conf = DEFAULT_DREMIO_CONFIG.withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);

  }

  @Test
  public void testSecretProviderLookupWithRawPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";
    DremioConfig conf = DEFAULT_DREMIO_CONFIG
      .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "SecretString%.isNotAValidURI")
      .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);

  }

  @Test
  public void testSecretProviderLookupWithDataUriPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";
    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "data:,dremio456")
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class, DataCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);

  }

  @Test
  public void testSecretProviderLookupWithEnvUriPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";
    // set env $ldap
    String ldapEnv = "ldap";
    MOCK_ENVIRONMENT.put(ldapEnv, originalString);

    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "env:".concat(ldapEnv))
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class, MockEnvCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);

  }

  @Test
  public void testSecretProviderLookupWithFileUriPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";

    String fileLoc = tempFolder.newFile("test.file").getAbsolutePath();

    // create the password file
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileLoc))) {
      writer.write("dremio345");
    }
    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "file://".concat(fileLoc))
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class, FileCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);


  }

  @Test
  public void testSecretProviderLookupWithSecretUriPassword() throws Exception {
    String originalString = "secretEncryptDecryptValidation";

    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "secret:dremio123")
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath());

    // emulate "dremio-admin encrypt secret"
    SimpleCredentialsService credentialsService = SimpleCredentialsService.newInstance(conf, ImmutableSet.of(SecretCredentialsProvider.class, FileCredentialsProvider.class));
    SecretCredentialsProvider credentialsProvider = credentialsService.findProvider(SecretCredentialsProvider.class);

    thrown.expect(SecretCredentialsException.class);
    credentialsProvider.encrypt(originalString);
  }

}
