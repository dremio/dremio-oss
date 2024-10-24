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
package com.dremio.plugins.gcs;

import static com.dremio.hadoop.security.alias.DremioCredentialProvider.DREMIO_SCHEME_PREFIX;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_STATUS_PARALLEL_ENABLE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.ConnectionSchema;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.gcs.GCSConf.AuthMode;
import com.dremio.plugins.util.ContainerFileSystem;
import com.dremio.plugins.util.ContainerNotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Filesystem that combines multiple buckets in a single facade. */
public class GoogleBucketFileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
  private static final Logger logger = LoggerFactory.getLogger(GoogleBucketFileSystem.class);

  private static final String EMPTY_STRING = "";
  private static final String CONF_ACCOUNT_EMAIL = "fs.gs.auth.service.account.email";
  private static final String CONF_PRIVATE_KEY_ID = "fs.gs.auth.service.account.private.key.id";
  private static final String CONF_PRIVATE_KEY = "fs.gs.auth.service.account.private.key";
  private static final String CONF_SERVICE_ACCT = "google.cloud.auth.service.account.enable";
  private static final String CONF_PROJECT_ID = "fs.gs.project.id";
  public static final String DREMIO_PROJECT_ID = "dremio.gcs.projectId";
  public static final String DREMIO_KEY_FILE = "dremio.gcs.use_keyfile";
  public static final String DREMIO_CLIENT_EMAIL = "dremio.gcs.clientEmail";
  public static final String DREMIO_CLIENT_ID = "dremio.gcs.clientId";
  public static final String DREMIO_PRIVATE_KEY_ID = "dremio.gcs.privateKeyId";
  public static final String DREMIO_PRIVATE_KEY = "dremio.gcs.privateKey";
  public static final String DREMIO_WHITELIST_BUCKETS = "dremio.gcs.whitelisted.buckets";
  public static final String DREMIO_BYPASS_AUTH_CONFIG_FOR_TESTING_WITH_URL =
      "dremio.gcs.bypassAuthConfigForTestingWithUrl";

  private static final List<String> UNIQUE_PROPERTIES =
      ImmutableList.<String>of(
          CONF_PROJECT_ID,
          CONF_ACCOUNT_EMAIL,
          CONF_PRIVATE_KEY_ID,
          CONF_PRIVATE_KEY,
          DREMIO_PROJECT_ID,
          DREMIO_KEY_FILE,
          DREMIO_CLIENT_EMAIL,
          DREMIO_CLIENT_ID,
          DREMIO_PRIVATE_KEY_ID,
          DREMIO_PRIVATE_KEY);

  private static final ConnectionSchema<GCSConf> SCHEMA = ConnectionSchema.getSchema(GCSConf.class);

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();
  private GCSConf connectionConf;
  private Supplier<Storage> storageProvider;
  private GCSAsyncClient client;

  static final Predicate<CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY =
      (input -> {
        final FileStatus status = input.getStatus();
        if (!status.isDirectory()) {
          return true;
        }
        return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName())
            .equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      });

  public GoogleBucketFileSystem() {
    super(DREMIO_GCS_SCHEME, "bucket", ELIMINATE_PARENT_DIRECTORY);
  }

  // Resolves private key from hadoop Configuration.
  private String getPrivateKey(Configuration conf) throws IOException {
    char[] privateKey = conf.getPassword(DREMIO_PRIVATE_KEY);
    if (privateKey == null) {
      privateKey = conf.getPassword(CONF_PRIVATE_KEY);
    }
    if (privateKey == null) {
      privateKey = new char[0];
    }
    return String.valueOf(privateKey).replaceAll("\\\\n", "\n");
  }

  // converts privateKey from source or hadoop config to a string/uri resolvable by further
  // Configuration.getPassword() call
  private String getPrivateKeyUri(Configuration conf) {
    if (!SecretRef.isNullOrEmpty(connectionConf.privateKey)) {
      return SecretRef.toConfiguration(connectionConf.privateKey, DREMIO_SCHEME_PREFIX);
    }

    String privateKeyUri = conf.get(DREMIO_PRIVATE_KEY, EMPTY_STRING);
    if (EMPTY_STRING.equals(privateKeyUri)) {
      privateKeyUri = conf.get(CONF_PRIVATE_KEY, EMPTY_STRING);
    }

    return StringUtils.prependIfMissingIgnoreCase(privateKeyUri, DREMIO_SCHEME_PREFIX);
  }

  @Override
  public long getTTL(com.dremio.io.file.FileSystem fileSystem, com.dremio.io.file.Path path) {
    // TODO: Implement fetching TTL for GoogleBucketFileSystem
    logger.error("Fetching TTL for GoogleBucketFileSystem is unavailable.");
    return -1;
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    GCSConf gcsConf = SCHEMA.newMessage();
    if (conf.getBoolean(DREMIO_KEY_FILE, false)) {
      gcsConf.authMode = AuthMode.SERVICE_ACCOUNT_KEYS;
      gcsConf.clientId = conf.get(DREMIO_CLIENT_ID, EMPTY_STRING);
      if (gcsConf.clientId.equals(EMPTY_STRING)) {
        gcsConf.clientId = conf.get("fs.gs.auth.client.id", EMPTY_STRING);
      }
      gcsConf.clientEmail = conf.get(DREMIO_CLIENT_EMAIL, EMPTY_STRING);
      if (gcsConf.clientEmail.equals(EMPTY_STRING)) {
        gcsConf.clientEmail = conf.get("fs.gs.auth.service.account.email", EMPTY_STRING);
      }
      gcsConf.privateKeyId = conf.get(DREMIO_PRIVATE_KEY_ID, EMPTY_STRING);
      if (gcsConf.privateKeyId.equals(EMPTY_STRING)) {
        gcsConf.privateKeyId = conf.get(CONF_PRIVATE_KEY_ID, EMPTY_STRING);
      }
    } else {
      gcsConf.authMode = AuthMode.AUTO;
    }

    gcsConf.projectId = conf.get(DREMIO_PROJECT_ID, EMPTY_STRING);
    if (gcsConf.projectId.equals(EMPTY_STRING)) {
      gcsConf.projectId = conf.get(CONF_PROJECT_ID, EMPTY_STRING);
    }
    gcsConf.asyncEnabled = true;
    gcsConf.bucketWhitelist = getWhiteListBuckets(conf);

    this.connectionConf = gcsConf;

    String bypassAuthConfigForTestingUrl = conf.get(DREMIO_BYPASS_AUTH_CONFIG_FOR_TESTING_WITH_URL);
    if (!Strings.isNullOrEmpty(bypassAuthConfigForTestingUrl)) {
      this.storageProvider =
          Suppliers.memoize(
              () -> {
                StorageOptions.Builder storageOptionsBuilder =
                    StorageOptions.getDefaultInstance().toBuilder();
                // Use provided test host
                storageOptionsBuilder.setHost(bypassAuthConfigForTestingUrl);
                if (!gcsConf.projectId.equals(EMPTY_STRING)) {
                  storageOptionsBuilder.setProjectId(gcsConf.projectId);
                }
                return storageOptionsBuilder.build().getService();
              });

      // Skip client setup that requires credentials
      this.client =
          new GCSAsyncClient("gbfs", null, bypassAuthConfigForTestingUrl, GcsApiType.JSON);
    } else {
      // both client and storage call createCredentials at read time to have the latest
      // resolved credentials in case of secret rotation
      this.storageProvider =
          Suppliers.memoizeWithExpiration(this::createStorage, 5, TimeUnit.MINUTES);
      this.client =
          new GCSAsyncClient(
              "gbfs", this::createCredentials, "https://storage.googleapis.com", GcsApiType.XML);
    }
  }

  private GoogleCredentials createCredentials() {
    try {
      switch (connectionConf.authMode) {
        case SERVICE_ACCOUNT_KEYS:
          ImmutableMap.Builder<String, String> connectionCreds = ImmutableMap.builder();
          connectionCreds
              .put("type", "service_account")
              .put("client_id", connectionConf.clientId)
              .put("client_email", connectionConf.clientEmail)
              .put("private_key", getPrivateKey(getConf()))
              .put("private_key_id", connectionConf.privateKeyId);

          if (connectionConf.projectId != null) {
            connectionCreds.put("project_id", connectionConf.projectId);
          }
          JsonObject connectionCredsJson = new JsonObject();
          for (Map.Entry<String, String> entry : connectionCreds.build().entrySet()) {
            connectionCredsJson.add(entry.getKey(), new JsonPrimitive(entry.getValue()));
          }
          InputStream is = new ByteArrayInputStream(connectionCredsJson.toString().getBytes());
          return GoogleCredentials.fromStream(is);

        case AUTO:
        default:
          return GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException ioe) {
      throw UserException.ioExceptionError(ioe)
          .message("Failure creating GCS connection.")
          .buildSilently();
    }
  }

  private Storage createStorage() {
    StorageOptions.Builder storageOptionsBuilder = StorageOptions.getDefaultInstance().toBuilder();
    storageOptionsBuilder.setCredentials(createCredentials());
    if (!connectionConf.projectId.equals(EMPTY_STRING)) {
      storageOptionsBuilder.setProjectId(connectionConf.projectId);
    }
    return storageOptionsBuilder.build().getService();
  }

  private List<String> getWhiteListBuckets(Configuration conf) {
    String bucketList = conf.get(DREMIO_WHITELIST_BUCKETS, "");
    return Arrays.stream(bucketList.split(","))
        .map(String::trim)
        .filter(input -> !Strings.isNullOrEmpty(input))
        .collect(Collectors.toList());
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {
    final Stream<String> bucketNames;
    if (connectionConf.bucketWhitelist != null && !connectionConf.bucketWhitelist.isEmpty()) {
      bucketNames = connectionConf.bucketWhitelist.stream();
    } else {
      try {
        bucketNames =
            StreamSupport.stream(
                    storageProvider
                        .get()
                        .list(BucketListOption.pageSize(100))
                        .iterateAll()
                        .spliterator(),
                    false)
                .map(b -> b.getName());
      } catch (StorageException se) {
        throw UserException.validationError(se).message("Failed to list buckets.").build(logger);
      }
    }

    return bucketNames.map(b -> new GCSContainerCreator(b));
  }

  private final class FileSystemSupplierImpl extends FileSystemSupplier {

    private final String containerName;
    private final Configuration parentConf;

    public FileSystemSupplierImpl(Configuration conf, String containerName) {
      this.parentConf = conf;
      this.containerName = containerName;
    }

    @Override
    public FileSystem create() throws IOException {
      final Configuration conf = new Configuration(parentConf);
      conf.set(
          GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
          parentConf.get(
              GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
              GoogleStoragePlugin.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE_DEFAULT));

      conf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
      conf.set(GCS_STATUS_PARALLEL_ENABLE.getKey(), "true");

      if (connectionConf.projectId != null) {
        conf.set(CONF_PROJECT_ID, connectionConf.projectId);
      }

      switch (connectionConf.authMode) {
        case SERVICE_ACCOUNT_KEYS:
          conf.set(CONF_ACCOUNT_EMAIL, connectionConf.clientEmail);
          conf.set(CONF_PRIVATE_KEY, getPrivateKeyUri(getConf()));
          conf.set(CONF_PRIVATE_KEY_ID, connectionConf.privateKeyId);
          conf.setBoolean(CONF_SERVICE_ACCT, true);
          break;
        case AUTO:
        default:
          conf.setBoolean(CONF_SERVICE_ACCT, true);
          break;
      }

      if (connectionConf.getProperties() != null) {
        for (Property p : connectionConf.getProperties()) {
          conf.set(p.name, p.value);
        }
      }
      return fsCache.get(new Path("gs://" + containerName + "/").toUri(), conf, UNIQUE_PROPERTIES);
    }
  }

  class GCSContainerCreator extends ContainerCreator {

    private final String name;

    public GCSContainerCreator(String name) {
      super();
      this.name = name;
    }

    @Override
    protected String getName() {
      return name;
    }

    @Override
    protected ContainerFileSystem.ContainerHolder toContainerHolder() throws IOException {
      return new ContainerFileSystem.ContainerHolder(
          name, new FileSystemSupplierImpl(getConf(), name));
    }
  }

  @Override
  protected ContainerHolder getUnknownContainer(String bucket) throws IOException {
    // run this to ensure we don't fail.
    try {
      storageProvider.get().list(bucket, BlobListOption.pageSize(1));
    } catch (StorageException e) {
      int status = e.getCode();
      throw new ContainerNotFoundException(
          String.format("Unable to find container %s - [%d %s]", bucket, status, e.getMessage()));
    }
    return new ContainerFileSystem.ContainerHolder(
        bucket, new FileSystemSupplierImpl(getConf(), bucket));
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version, Map<String, String> options)
      throws IOException {
    return client.newByteReader(path, version);
  }

  @Override
  public boolean supportsAsync() {
    return connectionConf.asyncEnabled;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(
          Arrays.<AutoCloseable>asList(client, () -> fsCache.closeAll(true), () -> super.close()));
    } catch (RuntimeException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
