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
package com.dremio.exec.store.hive.exec;

import static com.dremio.io.file.UriSchemes.AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_HDFS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.io.file.UriSchemes.GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.S3_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.dremio.common.FSConstants;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Wrapper file system used to work around class loader issues in Hive.
 *
 * The solution is to redirect S3/ADLS/Azure tables to this file system
 * and to create the actual fs(S3FileSystem etc) from here.
 *
 * Delegates all operations to the actual fs impl.
 *
 * Replaces class loader before any action to delegate all class loading to default
 * class loaders. This is to avoid loading non-hive/hadoop related classes here.
 */
public class DremioFileSystem extends FileSystem {

  private static final String FS_S3A_BUCKET = "fs.s3a.bucket.";
  private static final String FS_S3A_AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
  private com.dremio.io.file.FileSystem underLyingFs;
  private Path workingDir;
  private String scheme;
  private URI originalURI;
  private static final Map<String, Set<String>> SUPPORTED_SCHEME_MAP = ImmutableMap.of(
    DREMIO_S3_SCHEME, ImmutableSet.of("s3a", S3_SCHEME,"s3n", DREMIO_S3_SCHEME),
    DREMIO_GCS_SCHEME, ImmutableSet.of(GCS_SCHEME, DREMIO_GCS_SCHEME),
    DREMIO_AZURE_SCHEME, ImmutableSet.of(AZURE_SCHEME, "wasb", "abfs", "abfss"));

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    originalURI = name;
    switch (name.getScheme()) {
      case DREMIO_S3_SCHEME:
        conf.set(AsyncReaderUtils.FS_DREMIO_S3_IMPL, "com.dremio.plugins.s3.store.S3FileSystem");
        updateS3Properties(conf);
        break;
      case DREMIO_HDFS_SCHEME:
        conf.set(AsyncReaderUtils.FS_DREMIO_HDFS_IMPL, "org.apache.hadoop.hdfs.DistributedFileSystem");
        break;
      case DREMIO_AZURE_SCHEME:
        conf.set(AsyncReaderUtils.FS_DREMIO_AZURE_IMPL, "com.dremio.plugins.azure.AzureStorageFileSystem");
        updateAzureConfiguration(conf, name);
        break;
      case DREMIO_GCS_SCHEME:
        conf.set(AsyncReaderUtils.FS_DREMIO_GCS_IMPL, "com.dremio.plugins.gcs.GoogleBucketFileSystem");
        break;
      default:
        throw new UnsupportedOperationException("Unsupported async read path for hive parquet: " + name.getScheme());
    }
    try (Closeable swapper = swapClassLoader()) {
      underLyingFs = HadoopFileSystem.get(name, conf.iterator(), true);
    }
    workingDir = new Path(name).getParent();
    scheme = name.getScheme();
  }

  private void updateAzureConfiguration(Configuration conf, URI uri) {
    // default is key based, same as azure sources
    String accountName = getAccountNameFromURI(conf.get("authority"), uri);
    // strip any url information if any
    String accountNameWithoutSuffix = accountName.split("[.]")[0];
    conf.set("dremio.azure.account", accountNameWithoutSuffix);
    String authType = getAuthTypeForAccount(conf, accountName, accountNameWithoutSuffix);
    String key = null;

    String old_scheme = conf.get("old_scheme");
    if (old_scheme.equals(FileSystemUriSchemes.WASB_SCHEME) || old_scheme.equals(FileSystemUriSchemes.WASB_SECURE_SCHEME)) {
      conf.setIfUnset("dremio.azure.mode","STORAGE_V1");
    } else if (old_scheme.equals(FileSystemUriSchemes.ABFS_SCHEME) || old_scheme.equals(FileSystemUriSchemes.ABFS_SECURE_SCHEME)) {
      conf.setIfUnset("dremio.azure.mode","STORAGE_V2");
    }

    if (authType.equals(AuthType.SharedKey.name())) {
      key = getValueForProperty(conf, FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, accountName,
        accountNameWithoutSuffix, "Account Key not present in the configuration.");
      conf.set("dremio.azure.key", key);
      conf.set("dremio.azure.credentialsType", "ACCESS_KEY");
    } else if (authType.equals(AuthType.OAuth.name())) {
      updateOAuthConfig(conf, accountName, accountNameWithoutSuffix);
      conf.set("dremio.azure.credentialsType", "AZURE_ACTIVE_DIRECTORY");
    } else {
      throw new UnsupportedOperationException("This credentials type is not supported " + authType);
    }

  }

  private String getAuthTypeForAccount(Configuration conf, String accountName, String accountNameWithoutSuffix) {
    // try with entire authority (includes destination), fall back to account name without suffix
    String authType = conf.get(getAccountConfigurationName(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
      accountName), conf.get(getAccountConfigurationName(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
      accountNameWithoutSuffix)));
    if (authType != null) {
      return authType;
    }
    // fall back to property name without account info and a default value
    return conf.get(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey.name());
  }

  private String getAccountConfigurationName(String fsAzurePropertyName, String accountName) {
    return fsAzurePropertyName + "." + accountName;
  }

  void updateOAuthConfig(Configuration conf, String accountName, String accountNameWithoutSuffix) {
    final String CLIENT_ID = "dremio.azure.clientId";
    final String TOKEN_ENDPOINT = "dremio.azure.tokenEndpoint";
    final String CLIENT_SECRET = "dremio.azure.clientSecret";
    String refreshToken = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT,
      accountName, accountNameWithoutSuffix, "OAuth Client Endpoint not found.");
    String clientId = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID, accountName,
      accountNameWithoutSuffix, "OAuth Client Id not found.");
    String password = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET, accountName,
      accountNameWithoutSuffix ,"OAuth Client Password not found.");
    conf.set(CLIENT_ID, clientId);
    conf.set(TOKEN_ENDPOINT, refreshToken);
    conf.set(CLIENT_SECRET, password);
  }

  private String getValueForProperty(Configuration conf, String propertyName,
                                     String accountName, String accountNameWithoutSuffix,
                                     String errMsg) {
    String property =  conf.get(getAccountConfigurationName(propertyName, accountName),
      conf.get(getAccountConfigurationName(propertyName, accountNameWithoutSuffix)));
    if (property != null) {
      return property;
    }
    property = conf.get(propertyName);
    Preconditions.checkState(StringUtils.isNotEmpty(property), errMsg);
    return property;
  }

  private String getAccountNameFromURI(String authority, URI uri) {
    if (null == authority) {
      throw new IllegalArgumentException("Malformed URI : " + uri.toString());
    } else if (!authority.contains("@")) {
      throw new IllegalArgumentException("Malformed URI : " + uri.toString());
    } else {
      String[] authorityParts = authority.split("@", 2);
      if (authorityParts.length >= 2 && (authorityParts[0] == null || !authorityParts[0].isEmpty())) {
        return authorityParts[1];
      } else {
        String errMsg = String.format("'%s' has a malformed authority, expected container name. Authority takes the form abfs://[<container name>@]<account name>", uri.toString());
        throw new IllegalArgumentException(errMsg);
      }
    }
  }

  private void updateS3Properties(Configuration conf) {
    // Hadoop s3 supports a default list of 1. Basic Credentials
    // 2. Environment variables 3. Instance role
    // If provider is not set in configuration try to derive one of the three.
    if (StringUtils.isEmpty(getCredentialsProvider(conf)) ) {
       if (!StringUtils.isEmpty(getAccessKey(conf))) {
         conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
       } else if (System.getenv("AWS_ACCESS_KEY_ID") != null || System.getenv("AWS_ACCESS_KEY") != null) {
         String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
         if (accessKey == null) {
           accessKey = System.getenv("AWS_ACCESS_KEY");
         }

         String secretKey = System.getenv("AWS_SECRET_KEY");
         if (secretKey == null) {
           secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
         }
         conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
         conf.set(FSConstants.FS_S3A_ACCESS_KEY, accessKey);
         conf.set(FSConstants.FS_S3A_SECRET_KEY, secretKey);
       } else {
         conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "com.amazonaws.auth.InstanceProfileCredentialsProvider");
       }
    }
    // copy over bucket properties to source configuration
    updateSourcePropsFromBucketProps(conf);
  }

  private void updateSourcePropsFromBucketProps(Configuration conf) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".access.key";
    String bucketAccessKey = conf.get(bucketConfName);
    if (StringUtils.isNotEmpty(bucketAccessKey)) {
      // dremio s3 file system does not support bucket overrides
      conf.set(FSConstants.FS_S3A_ACCESS_KEY, bucketAccessKey);
    }
    String bucketSecretConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".secret.key";
    String bucketSecretKey = conf.get(bucketSecretConfName);
    if (StringUtils.isNotEmpty(bucketSecretKey)) {
      // dremio s3 file system does not support bucket overrides
      conf.set(FSConstants.FS_S3A_SECRET_KEY, bucketSecretKey);
    }

    String endpointForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() + ".endpoint");
    if (StringUtils.isNotEmpty(endpointForBucket)) {
      conf.set("fs.s3a.endpoint", endpointForBucket);
    }

    String credentialsProviderForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() +
      ".aws.credentials.provider");
    if (StringUtils.isNotEmpty(credentialsProviderForBucket)) {
      conf.set("fs.s3a.aws.credentials.provider", credentialsProviderForBucket);
    }

    String httpSchemeForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() +
      ".connection.ssl.enabled");
    if (StringUtils.isNotEmpty(httpSchemeForBucket)) {
      conf.set("fs.s3a.connection.ssl.enabled", httpSchemeForBucket);
    }
   }

  private String getAccessKey(Configuration conf) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".access.key";
    return conf.get(bucketConfName, conf.get(FSConstants.FS_S3A_ACCESS_KEY));
  }

  private String getCredentialsProvider(Configuration conf) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".aws.credentials" +
      ".provider";
    return conf.get(bucketConfName, conf.get(FS_S3A_AWS_CREDENTIALS_PROVIDER));
  }

  @Override
  public URI getUri() {
    if (scheme.equals(DREMIO_AZURE_SCHEME) || scheme.equals(DREMIO_S3_SCHEME) || scheme.equals(DREMIO_GCS_SCHEME)) {
      // GCS, Azure File System and S3 File system have modified URIs.
      return originalURI;
    }
    return underLyingFs.getUri();
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    FSInputStream fsInputStream = null;
    try (Closeable swapper = swapClassLoader()) {
      fsInputStream = underLyingFs.open(com.dremio.io.file.Path.of(path.toUri()));
    }
    return new FSDataInputStream(new FSInputStreamWrapper(fsInputStream));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int i, short i1, long l, Progressable progressable) throws IOException {
    FSOutputStream fsOutputStream = null;
    try (Closeable swapper = swapClassLoader()) {
      fsOutputStream = underLyingFs.create(com.dremio.io.file.Path.of(path.toUri()),
        overwrite);
    }
    return new FSDataOutputStream(fsOutputStream, null);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException("Append to a file not supported.");
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.rename(com.dremio.io.file.Path.of(path.toUri()), com.dremio.io.file.Path.of(path1.toUri()));
    }
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.delete(com.dremio.io.file.Path.of(path.toUri()), recursive);
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    final List<FileStatus> fileStatusList = Lists.newArrayList();
    com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(path.toUri());
    DirectoryStream<FileAttributes> attributes = null;
    long defaultBlockSize;
    try (Closeable swapper = swapClassLoader()) {
      attributes = underLyingFs.list(dremioPath);
      defaultBlockSize = underLyingFs.getDefaultBlockSize(dremioPath);
      attributes.forEach(attribute -> fileStatusList.add(getFileStatusFromAttributes(attribute, defaultBlockSize)));
      return fileStatusList.toArray(new FileStatus[0]);
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    this.workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    String fsPerms = fsPermission.toString();
    Set<PosixFilePermission> posixFilePermission =
      PosixFilePermissions.fromString(fsPerms.substring(1, fsPerms.length()));
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.mkdirs(com.dremio.io.file.Path.of(path.toUri()), posixFilePermission);
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(path.toUri());
    FileAttributes attributes = null;
    long defaultBlockSize;
    try (Closeable swapper = swapClassLoader()) {
      attributes = underLyingFs.getFileAttributes(dremioPath);
      defaultBlockSize = underLyingFs.getDefaultBlockSize(dremioPath);
    }
    return getFileStatusFromAttributes(path, attributes, defaultBlockSize);
  }

  @Override
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String thatScheme = uri.getScheme();
    if (thatScheme == null) {              // fs is relative
      return;
    }
    URI thisUri = getCanonicalUri();
    String thisScheme = thisUri.getScheme();
    //authority and scheme are not case sensitive
    if (thisScheme.equalsIgnoreCase(thatScheme) ||
      (SUPPORTED_SCHEME_MAP.containsKey(thisScheme)
        && SUPPORTED_SCHEME_MAP.get(thisScheme).contains(thatScheme))) {// schemes match
      String thisAuthority = thisUri.getAuthority();
      String thatAuthority = uri.getAuthority();
      if (thatAuthority == null &&                // path's authority is null
        thisAuthority != null) {                // fs has an authority
        URI defaultUri = getDefaultUri(getConf());
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())) {
          uri = defaultUri; // schemes match, so use this uri instead
        } else {
          uri = null; // can't determine auth of the path
        }
      }
      if (uri != null) {
        // canonicalize uri before comparing with this fs
        uri = canonicalizeUri(uri);
        thatAuthority = uri.getAuthority();
        if (thisAuthority == thatAuthority ||       // authorities match
          (thisAuthority != null &&
            thisAuthority.equalsIgnoreCase(thatAuthority))) {
          return;
        }
      }
    }
    throw new IllegalArgumentException("Wrong FS: " + path +
      ", expected: " + this.getUri());
  }

  @Deprecated
  private FileStatus getFileStatusFromAttributes(Path path, FileAttributes attributes,
                                                 long defaultBlockSize) {
    return new FileStatus(attributes.size(), attributes.isDirectory(), 1,
      defaultBlockSize, attributes.lastModifiedTime().toMillis(), path);
  }

  private FileStatus getFileStatusFromAttributes(FileAttributes attributes,
                                                 long defaultBlockSize) {
    return new FileStatus(attributes.size(), attributes.isDirectory(), 1,
            defaultBlockSize, attributes.lastModifiedTime().toMillis(), new Path(String.valueOf(attributes.getPath())));
  }

  public boolean supportsAsync() {
    return underLyingFs.supportsAsync();
  }

  public AsyncByteReader getAsyncByteReader(AsyncByteReader.FileKey fileKey, OperatorStats operatorStats, Map<String, String> options) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      if (underLyingFs instanceof HadoopFileSystem) {
        return ((HadoopFileSystem) underLyingFs).getAsyncByteReader(fileKey, operatorStats, options);
      } else {
        return underLyingFs.getAsyncByteReader(fileKey,options);
      }
    }
  }

  @Override
  public String getScheme() {
    return underLyingFs.getScheme();
  }

  /**
   * swaps current threads class loader to use application class loader
   * @return
   */
  private Closeable swapClassLoader() {
    return ContextClassLoaderSwapper.swapClassLoader(HadoopFileSystem.class);
  }

}
