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
package com.dremio.dac.homefiles;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.NotBlank;

import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.MayBeDistFileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.proto.DataCredentials;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Source type used for Home Files purposes.
 */
@SourceType(value = "HOME", configurable = false)
public class HomeFileConf extends MayBeDistFileSystemConf<HomeFileConf, HomeFileSystemStoragePlugin> {

  private static final String UPLOADS = "_uploads";
  private static final String STAGING = "_staging";

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(HomeFileConf.class);

  // this is transient since it is actually different for each node (which means it is property based as opposed to defined directly in this configuration).
  private transient String hostname;

  private final transient Supplier<URI> uri = new Supplier<URI>() {
    @Override
    public URI get() {
      try {
        return new URI(location);
      } catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }};

  @NotBlank
  @Tag(1)
  public String location;

  @Tag(2)
  public boolean enableAsync = true;

  @Tag(3)
  public String accessKey = null;

  @Tag(4)
  public String secretKey = null;

  @Tag(5)
  public String iamRole = null;

  @Tag(6)
  public String externalId = null;

  @Tag(7)
  public String tokenEndpoint = null;

  @Tag(8)
  public String clientId = null;

  @Tag(9)
  public String clientSecret = null;

  @Tag(10)
  public String accountName = null;

  @Tag(11)
  public String accountKind = null;

  @Tag(12)
  public String sharedAccessKey = null;
  //Tag has been deprecated please do not use.

  public HomeFileConf() {

  }

  public HomeFileConf(String location, String thisNode, boolean enableAsync, DataCredentials dataCredentials) {
    this(location, thisNode);
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
    URI uri,
    String thisNode,
    SchemaMutability mutability,
    MetadataPolicy policy,
    boolean enableAsync,
    DataCredentials dataCredentials
  ) {
    SourceConfig conf = new SourceConfig();

    HomeFileConf fc = new HomeFileConf(uri.toString(), thisNode, enableAsync, dataCredentials);
    conf.setConnectionConf(fc);
    conf.setMetadataPolicy(policy);
    conf.setName(name);
    return conf;
  }

  public HomeFileConf(String location) {
    this.location = location;
  }

  public HomeFileConf(String location, String hostname) {
    this.location = location;
    this.hostname = hostname;
  }

  @Override
  public Path getPath() {
    return Path.of(uri.get().getPath());
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  public boolean isPdfsBased() {
    return uri.get().getScheme().equals("pdfs");
  }

  @Override
  public List<Property> getProperties() {
    return ImmutableList.of();
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    URI path = uri.get();
    if(path.getAuthority() != null) {
      return path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      return path.getScheme() + ":///";
    }

  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_VIEW;
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
  public HomeFileSystemStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new HomeFileSystemStoragePlugin(this, context, name, pluginIdProvider);
  }

  public Path getBaseUploadsPath() {
    return getPath();
  }

  public Path getInnerUploads() {
    return getPath().resolve(UPLOADS);
  }

  public Path getStagingPath(String hostname) {
    return getPath().resolve(STAGING + "." + hostname);
  }

  public FileSystem getFilesystemAndCreatePaths(String hostname) throws IOException {
    FileSystem fs = HadoopFileSystem.get(uri.get(), new Configuration(), enableAsync);
    fs.mkdirs(getPath(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
    fs.mkdirs(getInnerUploads(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);

    if(hostname == null) {
      hostname = this.hostname;
    }

    if(hostname != null) {
      final Path stagingDir = getStagingPath(hostname);
      fs.mkdirs(stagingDir, HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
      FileSystemUtils.deleteOnExit(fs, stagingDir);
    }
    return fs;
  }

  @Override
  public boolean isInternal() {
    return true;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }
}
