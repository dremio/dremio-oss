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
package com.dremio.exec.catalog;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.service.namespace.SupportsDecoratingSecrets;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.services.credentials.CredentialsService;
import io.protostuff.ByteString;
import java.util.Map;
import javax.inject.Provider;

/**
 * Provides CredentialsService to SecretRefs within ConnectionConfs such that encrypted secrets or
 * secret uris can be resolved.
 */
public class ConnectionReaderDecorator implements ConnectionReader {
  private final ConnectionReader delegate;
  private final Provider<CredentialsService> credentialsService;

  public ConnectionReaderDecorator(
      ConnectionReader delegate, Provider<CredentialsService> credentialsService) {
    this.delegate = delegate;
    this.credentialsService = credentialsService;
  }

  @Override
  public ConnectionConf<?, ?> getConnectionConf(SourceConfig config) {
    return delegate.getConnectionConf(config).decorateSecrets(credentialsService.get());
  }

  @Override
  public String toStringWithoutSecrets(SourceConfig sourceConfig) {
    return delegate.toStringWithoutSecrets(sourceConfig);
  }

  @Override
  public Map<String, Class<? extends ConnectionConf<?, ?>>> getAllConnectionConfs() {
    return delegate.getAllConnectionConfs();
  }

  @Override
  public <T extends AbstractConnectionConf> T getConnectionConf(String typeName, ByteString bytes) {
    final T conf = delegate.getConnectionConf(typeName, bytes);
    if (conf instanceof SupportsDecoratingSecrets) {
      ((SupportsDecoratingSecrets) conf)
          .decorateSecrets(credentialsService.get()); // Mutate the conf
    }
    return conf;
  }
}
