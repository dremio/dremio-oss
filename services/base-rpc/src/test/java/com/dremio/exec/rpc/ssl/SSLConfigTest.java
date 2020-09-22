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
package com.dremio.exec.rpc.ssl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.KeyStore;
import java.util.Optional;
import java.util.Properties;

import org.junit.Test;

import com.dremio.ssl.SSLConfig;

/**
 * Tests {@link SSLConfig}.
 */
public class SSLConfigTest {

  private static void checkDefaults(SSLConfig sslConfig) {
    assertEquals(KeyStore.getDefaultType(), sslConfig.getKeyStoreType());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyStorePath());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyStorePassword());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyPassword());

    assertEquals(KeyStore.getDefaultType(), sslConfig.getTrustStoreType());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getTrustStorePath());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getTrustStorePassword());

    assertFalse(sslConfig.disablePeerVerification());
    assertFalse(sslConfig.disableHostVerification());
  }

  @Test
  public void checkDefaults() {
    final SSLConfig sslConfig = SSLConfig.newBuilder().build();

    checkDefaults(sslConfig);
  }

  @Test
  public void checkKeyPassword() {
    {
      final String implicit = "implicit";
      final SSLConfig config = SSLConfig.newBuilder()
          .setKeyStorePassword(implicit)
          .build();

      assertEquals(implicit, config.getKeyStorePassword());
      assertEquals(implicit, config.getKeyPassword());
    }

    {
      final String explicit = "explicit";
      final SSLConfig config = SSLConfig.newBuilder()
          .setKeyPassword(explicit)
          .build();

      assertEquals(SSLConfig.UNSPECIFIED, config.getKeyStorePassword());
      assertEquals(explicit, config.getKeyPassword());
    }
  }

  @Test
  public void fromProperties() {
    {
      assertFalse(SSLConfig.of(null).isPresent());
    }

    {
      assertFalse(SSLConfig.of(
          new Properties() {{
            setProperty(SSLConfig.TRUST_STORE_PATH, "not enabled");
            setProperty(SSLConfig.TRUST_STORE_PASSWORD, "not enabled");
          }}
      ).isPresent());
    }

    {
      final Optional<SSLConfig> config = SSLConfig.of(
          new Properties() {{
            setProperty(SSLConfig.ENABLE_SSL, "true");
          }}
      );

      assertTrue(config.isPresent());
      checkDefaults(config.get());
    }

    {
      final Optional<SSLConfig> config = SSLConfig.of(
          new Properties() {{
            setProperty(SSLConfig.ENABLE_SSL, "false");
            setProperty(SSLConfig.TRUST_STORE_PATH, "not enabled");
            setProperty(SSLConfig.TRUST_STORE_PASSWORD, "not enabled");
          }}
      );

      assertFalse(config.isPresent());
    }

    {
      final String path = "path";
      final String password = "password";
      final Optional<SSLConfig> config = SSLConfig.of(
          new Properties() {{
            setProperty(SSLConfig.ENABLE_SSL, "true");
            setProperty(SSLConfig.TRUST_STORE_PATH, path);
            setProperty(SSLConfig.TRUST_STORE_PASSWORD, password);
            setProperty(SSLConfig.DISABLE_CERT_VERIFICATION, "true");
          }}
      );

      assertTrue(config.isPresent());
      assertEquals(path, config.get().getTrustStorePath());
      assertEquals(password, config.get().getTrustStorePassword());
      assertTrue(config.get().disablePeerVerification());
      assertFalse(config.get().disableHostVerification());
    }

    {
      final String path = "p4Th";
      final String password = "p4sSw0rd";
      final Optional<SSLConfig> config = SSLConfig.of(
          new Properties() {{
            setProperty("SSL", "true");
            setProperty("tRustSTore", path);
            setProperty("TRUSTSTOREPASSWORD", password);
            setProperty("disablehostverification", "true");
          }}
      );

      assertTrue(config.isPresent());
      assertEquals(path, config.get().getTrustStorePath());
      assertEquals(password, config.get().getTrustStorePassword());
      assertFalse(config.get().disablePeerVerification());
      assertTrue(config.get().disableHostVerification());
    }
  }
}
