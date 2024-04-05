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

import com.dremio.ssl.SSLConfig;
import java.security.KeyStore;
import java.util.Optional;
import java.util.Properties;
import org.junit.Test;

/** Tests {@link SSLConfig}. */
public class SSLConfigTest {

  private static void checkDefaultsForClient(SSLConfig sslConfig) {
    assertEquals(KeyStore.getDefaultType(), sslConfig.getKeyStoreType());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyStorePath());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyStorePassword());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getKeyPassword());

    assertEquals(KeyStore.getDefaultType(), sslConfig.getTrustStoreType());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getTrustStorePath());
    assertEquals(SSLConfig.UNSPECIFIED, sslConfig.getTrustStorePassword());

    assertFalse(sslConfig.disablePeerVerification());
    assertFalse(sslConfig.disableHostVerification());

    assertTrue(sslConfig.useSystemTrustStore());
  }

  @Test
  public void checkDefaultsForClient() {
    final SSLConfig sslConfig = SSLConfig.newBuilderForClient().build();

    checkDefaultsForClient(sslConfig);
  }

  @Test
  public void checkDefaultsForServer() {
    final SSLConfig sslConfig = SSLConfig.newBuilderForServer().build();
    assertFalse(sslConfig.useSystemTrustStore());
  }

  @Test
  public void checkKeyPassword() {
    {
      final String implicit = "implicit";
      final SSLConfig config =
          SSLConfig.newBuilderForClient().setKeyStorePassword(implicit).build();

      assertEquals(implicit, config.getKeyStorePassword());
      assertEquals(implicit, config.getKeyPassword());
    }

    {
      final String explicit = "explicit";
      final SSLConfig config = SSLConfig.newBuilderForClient().setKeyPassword(explicit).build();

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
      assertFalse(
          SSLConfig.of(
                  new Properties() {
                    {
                      setProperty(SSLConfig.TRUST_STORE_PATH, "not enabled");
                      setProperty(SSLConfig.TRUST_STORE_PASSWORD, "not enabled");
                    }
                  })
              .isPresent());
    }

    {
      final Optional<SSLConfig> config =
          SSLConfig.of(
              new Properties() {
                {
                  setProperty(SSLConfig.ENABLE_SSL, "true");
                }
              });

      assertTrue(config.isPresent());
      assertTrue(config.get().useSystemTrustStore());
      checkDefaultsForClient(config.get());
    }

    {
      final Optional<SSLConfig> config =
          SSLConfig.of(
              new Properties() {
                {
                  setProperty(SSLConfig.ENABLE_SSL, "false");
                  setProperty(SSLConfig.TRUST_STORE_PATH, "not enabled");
                  setProperty(SSLConfig.TRUST_STORE_PASSWORD, "not enabled");
                }
              });

      assertFalse(config.isPresent());
    }

    {
      final String path = "path";
      final String password = "password";
      final Optional<SSLConfig> config =
          SSLConfig.of(
              new Properties() {
                {
                  setProperty(SSLConfig.ENABLE_SSL, "true");
                  setProperty(SSLConfig.TRUST_STORE_PATH, path);
                  setProperty(SSLConfig.TRUST_STORE_PASSWORD, password);
                  setProperty(SSLConfig.DISABLE_CERT_VERIFICATION, "true");
                  setProperty(SSLConfig.USE_SYSTEM_TRUST_STORE, "true");
                }
              });

      assertTrue(config.isPresent());
      assertEquals(path, config.get().getTrustStorePath());
      assertEquals(password, config.get().getTrustStorePassword());
      assertTrue(config.get().disablePeerVerification());
      assertFalse(config.get().disableHostVerification());
      assertTrue(config.get().useSystemTrustStore());
    }

    {
      final String path = "p4Th";
      final String password = "p4sSw0rd";
      final Optional<SSLConfig> config =
          SSLConfig.of(
              new Properties() {
                {
                  setProperty("SSL", "true");
                  setProperty("tRustSTore", path);
                  setProperty("TRUSTSTOREPASSWORD", password);
                  setProperty("disablehostverification", "true");
                  setProperty("useSystemTrustStore", "false");
                }
              });

      assertTrue(config.isPresent());
      assertEquals(path, config.get().getTrustStorePath());
      assertEquals(password, config.get().getTrustStorePassword());
      assertFalse(config.get().disablePeerVerification());
      assertTrue(config.get().disableHostVerification());
      assertFalse(config.get().useSystemTrustStore());
    }
  }
}
