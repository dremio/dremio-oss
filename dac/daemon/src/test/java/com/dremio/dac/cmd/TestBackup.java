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
package com.dremio.dac.cmd;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.dremio.config.DremioConfig;
import com.dremio.dac.cmd.Backup.WebClientFactory;
import com.dremio.dac.server.DACConfig;
import com.dremio.services.credentials.CredentialsService;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.inject.Provider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestBackup {

  @BeforeEach
  void setup() {
    // reset webclient factory before each test
    Backup.setWebClientFactory(Backup.defaultWebClientFactory);
  }

  @Test
  @DisplayName("--accept-all does not require ssl verification")
  void acceptAll() throws GeneralSecurityException, IOException {
    WebClient mockClient = mock(WebClient.class);
    WebClientFactory factory = spy(mockedFactory(mockClient));
    Backup.setWebClientFactory(factory);

    Backup.doMain(
        new String[] {"-u", "dremio", "-p", "somepass", "--accept-all", "-d", "/somepath"},
        createDacConfig());

    verify(factory)
        .createClient(
            any(DACConfig.class),
            any(),
            any(String.class),
            any(String.class),
            eq(false)); // do not check ssl
  }

  @Test
  @DisplayName("--accept-all does require ssl verification")
  void notAcceptAll() throws GeneralSecurityException, IOException {
    WebClient mockClient = mock(WebClient.class);
    WebClientFactory factory = spy(mockedFactory(mockClient));
    Backup.setWebClientFactory(factory);

    Backup.doMain(
        new String[] {
          "-u", "dremio",
          "-p", "somepass",
          "-d", "/somepath"
        },
        createDacConfig());

    verify(factory)
        .createClient(
            any(DACConfig.class),
            any(),
            any(String.class),
            any(String.class),
            eq(true)); // check ssl
  }

  private static @NotNull WebClientFactory mockedFactory(WebClient mockClient) {
    return new WebClientFactory() {
      @SuppressWarnings("RedundantThrows")
      @Override
      public WebClient createClient(
          DACConfig dacConfig,
          Provider<CredentialsService> credentialsServiceProvider,
          String userName,
          String password,
          boolean checkSSLCertificates)
          throws GeneralSecurityException, IOException {
        return mockClient;
      }
    };
  }

  private DACConfig createDacConfig() {
    return new DACConfig(DremioConfig.create());
  }
}
