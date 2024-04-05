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
package com.dremio.exec.store.dfs;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.proto.DataCredentials;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.protostuff.Tag;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.inject.Provider;

/** Source type used for internal purposes. */
@SourceType(value = "INTERNAL", configurable = false)
public class InternalFileConf
    extends MayBeDistFileSystemConf<InternalFileConf, MayBeDistFileSystemPlugin<InternalFileConf>> {

  @Tag(1)
  public String connection;

  @Tag(2)
  public String path = "/";

  @Tag(3)
  public boolean enableImpersonation = false;

  @Tag(4)
  public List<Property> propertyList = Lists.newArrayList();

  @Tag(5)
  public SchemaMutability mutability = SchemaMutability.NONE;

  @Tag(6)
  public boolean isInternal = true;

  @Tag(7)
  public boolean enableAsync = true;

  @Tag(8)
  public String accessKey = null;

  @Tag(9)
  public String secretKey = null;

  @Tag(10)
  public String iamRole = null;

  @Tag(11)
  public String externalId = null;

  @Tag(12)
  public DefaultCtasFormatSelection defaultCtasFormat = DefaultCtasFormatSelection.PARQUET;

  @Tag(13)
  public String tokenEndpoint = null;

  @Tag(14)
  public String clientId = null;

  @Tag(15)
  public String clientSecret = null;

  @Tag(16)
  public String accountName = null;

  @Tag(17)
  public boolean isPartitionInferenceEnabled = false;

  @Tag(18)
  public String accountKind = null;

  @Tag(19)
  public String sharedAccessKey = null;

  // Tag has been deprecated please do not use.

  @Override
  public Path getPath() {
    return Path.of(path);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return enableImpersonation;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return isPartitionInferenceEnabled;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    return connection;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return mutability;
  }

  @Override
  public String getAccessKey() {
    return accessKey;
  }

  @Override
  public String getSecretKey() {
    return secretKey;
  }

  @Override
  public String getIamRole() {
    return iamRole;
  }

  @Override
  public String getExternalId() {
    return externalId;
  }

  @Override
  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public String getClientSecret() {
    return clientSecret;
  }

  @Override
  public String getAccountName() {
    return accountName;
  }

  @Override
  public String getAccountKind() {
    return accountKind;
  }

  @Override
  public MayBeDistFileSystemPlugin<InternalFileConf> newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new MayBeDistFileSystemPlugin<>(this, context, name, pluginIdProvider);
  }

  public InternalFileConf() {}

  InternalFileConf(
      String connection,
      String path,
      boolean enableImpersonation,
      List<Property> propertyList,
      SchemaMutability mutability,
      boolean enableAsync,
      DataCredentials dataCredentials) {
    this.connection = connection;
    this.path = path;
    this.enableImpersonation = enableImpersonation;
    this.propertyList = propertyList;
    this.mutability = mutability;
    this.enableAsync = enableAsync;
    if (dataCredentials != null) {
      if (dataCredentials.hasKeys()) {
        this.accessKey = dataCredentials.getKeys().getAccessKey();
        this.secretKey = dataCredentials.getKeys().getSecretKey();
      } else if (dataCredentials.hasDataRole()) {
        this.iamRole = dataCredentials.getDataRole().getIamRole();
        this.externalId = dataCredentials.getDataRole().getExternalId();
      } else if (dataCredentials.hasClientAccess()) {
        this.tokenEndpoint = dataCredentials.getClientAccess().getTokenEndpoint();
        this.clientId = dataCredentials.getClientAccess().getClientId();
        this.clientSecret = dataCredentials.getClientAccess().getClientSecret();
        this.accountName = dataCredentials.getClientAccess().getAccountName();
        this.accountKind = dataCredentials.getClientAccess().getAccountKind();
      }
    }
  }

  public static SourceConfig create(
      String name,
      URI path,
      SchemaMutability mutability,
      MetadataPolicy policy,
      boolean enableAsync,
      DataCredentials dataCredentials) {
    SourceConfig conf = new SourceConfig();
    final String connection;
    if (path.getAuthority() != null) {
      connection = path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      connection = path.getScheme() + ":///";
    }

    InternalFileConf fc =
        new InternalFileConf(
            connection, path.getPath(), false, null, mutability, enableAsync, dataCredentials);
    conf.setConnectionConf(fc);
    conf.setMetadataPolicy(policy);
    conf.setName(name);
    return conf;
  }

  @Override
  public boolean createIfMissing() {
    return !connection.startsWith("classpath:");
  }

  @Override
  public boolean isInternal() {
    return isInternal;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }

  private final transient Supplier<URI> uri =
      new Supplier<URI>() {
        @Override
        public URI get() {
          try {
            return new URI(connection);
          } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
          }
        }
      };

  public boolean isPdfsBased() {
    return uri.get().getScheme().equals("pdfs");
  }

  @Override
  public String getDefaultCtasFormat() {
    return defaultCtasFormat.getDefaultCtasFormat();
  }
}
