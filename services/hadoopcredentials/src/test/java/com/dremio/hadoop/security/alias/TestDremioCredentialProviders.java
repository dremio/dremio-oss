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
package com.dremio.hadoop.security.alias;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static com.dremio.test.DremioTest.DEFAULT_DREMIO_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.services.credentials.CredentialsService;

/**
 * Test the Dremio Credential Provider Factory which extends the Hadoop library.
 */
public class TestDremioCredentialProviders {
  private static final String HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";
  private static final String SECRET_KEY = "a.b.c.key";
  private CredentialsService credentialsService;

  @Before
  public void setUp() throws Exception {
    credentialsService = CredentialsService.newInstance(DEFAULT_DREMIO_CONFIG, CLASSPATH_SCAN_RESULT);
    DremioCredentialProviderFactory.configure(() -> credentialsService);
  }
  @Test
  public void testDremioDataProvider() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "dremio+data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==");

    assertEquals("Hello, World!", new String(conf.getPassword(SECRET_KEY)));
  }

  @Test
  public void missingDremioScheme() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==");

    assertEquals("Due to fallback, a secret URI without proper dremio+ scheme will output as it is",
      "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==", new String(conf.getPassword(SECRET_KEY)));
  }
  @Test
  public void testClearTextPasswordFallback() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "Hello, World!");

    assertEquals("Hello, World!", new String(conf.getPassword(SECRET_KEY)));
  }

  @Test
  public void testClearTextPasswordFallback2() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "abc123");

    assertEquals("abc123", new String(conf.getPassword(SECRET_KEY)));
  }


  @Test
  public void testSchemeCaseInsensitivity() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "DreMio+Data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==");

    assertEquals("Hello, World!", new String(conf.getPassword(SECRET_KEY)));
  }

  @Test
  public void invalidCredentialProviderScheme() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "dremio+invalidscheme:text/plain;base64,SGVsbG8sIFdvcmxkIQ==");

    assertThatThrownBy(() -> conf.getPassword(SECRET_KEY))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessageContaining("Unable to find a suitable credentials provider for invalidscheme");
  }

  @Test
  public void clearTextPasswordWithDremioScheme() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "dremio:///");
    conf.set(SECRET_KEY, "dremio+data:abc123");

    assertEquals("If an issue occurred when Dremio is resolving the secret URI, we will fallback to cleartext.",
      "dremio+data:abc123", new String(conf.getPassword(SECRET_KEY)));
  }
}
