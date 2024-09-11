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

import com.dremio.services.credentials.CredentialsService;
import java.io.IOException;
import java.net.URI;
import javax.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

/** The Dremio factory to create Credential Providers, which is used by the Hadoop ServiceLoader. */
public class DremioCredentialProviderFactory extends CredentialProviderFactory {
  public static final String DREMIO_SCHEME = "dremio";

  private static Provider<CredentialsService> credentialsServiceProvider;

  @Override
  public CredentialProvider createProvider(URI providerName, Configuration conf) {
    if (DREMIO_SCHEME.equalsIgnoreCase(providerName.getScheme())) {
      return new DremioCredentialProvider(credentialsServiceProvider.get(), conf);
    }
    return null;
  }

  /**
   * Called by DremioAWSCredentialsProvider, which runs under Hive class loader and can only use
   * core java classes
   *
   * @param alias the name of a specific credential
   * @param pattern credential pattern to resolve
   * @return resolved secret.
   * @throws IOException if secret resolution fails.
   */
  public static char[] resolveCredentialPattern(String alias, String pattern) throws IOException {
    return new DremioCredentialProvider(credentialsServiceProvider.get(), null)
        .resolveCredentialPattern(alias, pattern);
  }

  /**
   * Called by Dremio Boostrap to bind DremioCredentialProviderFactory with CredentialsService
   * Provider.
   */
  public static void configure(Provider<CredentialsService> credentialsServiceProvider) {
    DremioCredentialProviderFactory.credentialsServiceProvider = credentialsServiceProvider;
  }
}
