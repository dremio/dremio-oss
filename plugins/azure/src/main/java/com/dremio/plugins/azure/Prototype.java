/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.plugins.azure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;

class Prototype {

  public static final Prototype WASB = new Prototype(
      "http",
      "blob.core.windows.net",
      FileSystemUriSchemes.WASB_SCHEME,
      NativeAzureFileSystem.class,
      true
      );

  public static final Prototype WASBS = new Prototype(
      "https",
      "blob.core.windows.net",
      FileSystemUriSchemes.WASB_SECURE_SCHEME,
      NativeAzureFileSystem.Secure.class,
      true
      );

  public static final Prototype ABFS = new Prototype(
      "http",
      "dfs.core.windows.net",
      FileSystemUriSchemes.ABFS_SCHEME,
      AzureBlobFileSystem.class,
      false
      );

  public static final Prototype ABFSS = new Prototype(
      "https",
      "dfs.core.windows.net",
      FileSystemUriSchemes.ABFS_SECURE_SCHEME,
      SecureAzureBlobFileSystem.class,
      false
      );

  private final String endpointScheme;
  private final String endpointSuffix;
  private final String scheme;
  private final Class<? extends FileSystem> fsImpl;
  private final boolean legacyMode;

  public Prototype(String endpointScheme, String endpointSuffix, String scheme, Class<? extends FileSystem> fsImpl, boolean legacyMode) {
    this.endpointScheme = endpointScheme;
    this.endpointSuffix = endpointSuffix;
    this.scheme = scheme;
    this.fsImpl = fsImpl;
    this.legacyMode = legacyMode;
  }

  public String getScheme() {
    return scheme;
  }

  public Class<? extends FileSystem> getFsImpl() {
    return fsImpl;
  }

  public String getEndpointScheme() {
    return endpointScheme;
  }

  public String getEndpointSuffix() {
    return endpointSuffix;
  }

  public void setImpl(Configuration conf, String account, String container, String key) {
    conf.set(String.format("fs.%s.impl", scheme), fsImpl.getName());
    if(legacyMode) {
      conf.set(String.format("fs.azure.account.key.%s.blob.core.windows.net", account), key);
      final String location = String.format("%s://%s@%s.blob.core.windows.net/", scheme, container, account);
      FileSystem.setDefaultUri(conf, new Path(location).toUri());
    } else {
      conf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, key);
      final String location = String.format("%s://%s@%s.dfs.core.windows.net/", scheme, container, account);
      FileSystem.setDefaultUri(conf, new Path(location).toUri());
    }
  }

}
