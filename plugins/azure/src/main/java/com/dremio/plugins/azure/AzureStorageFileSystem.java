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
package com.dremio.plugins.azure;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;
import static com.dremio.plugins.azure.AzureAuthenticationType.ACCESS_KEY;
import static com.dremio.plugins.azure.AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.azure.AzureStorageConf.AccountKind;
import com.dremio.plugins.util.ContainerFileSystem;
import com.microsoft.azure.storage.v10.adlsg2.models.DataLakeStorageErrorException;

import io.netty.buffer.ByteBuf;
import io.reactivex.Flowable;

/**
 * A container file system implementation for Azure Storage.
 *
 * Supports both Blob containers and Hierarchical containers
 */
public class AzureStorageFileSystem extends ContainerFileSystem implements MayProvideAsyncStream {

  private static final Logger logger = LoggerFactory.getLogger(AzureStorageFileSystem.class);

  static final String SCHEME = "dremioAzureStorage://";
  private static final String CONTAINER_HUMAN_NAME = "Container";
  static final String ACCOUNT = "dremio.azure.account";
  static final String KEY = "dremio.azure.key";
  static final String MODE = "dremio.azure.mode";
  static final String SECURE = "dremio.azure.secure";
  static final String CONTAINER_LIST = "dremio.azure.container_list";

  static final String CREDENTIALS_TYPE = "dremio.azure.credentialsType";

  static final String CLIENT_ID = "dremio.azure.clientId";
  static final String TOKEN_ENDPOINT = "dremio.azure.tokenEndpoint";
  static final String CLIENT_SECRET = "dremio.azure.clientSecret";
  static final String AZURE_ENDPOINT = "fs.azure.endpoint";

  private String azureEndpoint;
  private String account;
  private String key;

  private AzureAuthenticationType credentialsType;

  private String clientID;
  private String tokenEndpoint;
  private String clientSecret;

  private boolean secure;
  private AccountKind accountKind;
  private Prototype proto;
  private Configuration parentConf;
  private ContainerProvider blobProvider;
  private DataLakeG2Client client;

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();

  @Override
  public void close() throws IOException {
    fsCache.closeAll(true);
    super.close();
  }

  protected AzureStorageFileSystem() {
    super(SCHEME, CONTAINER_HUMAN_NAME, a -> true);
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    parentConf = conf;
    try {
      conf.setIfUnset(CREDENTIALS_TYPE, ACCESS_KEY.name());
      credentialsType = AzureAuthenticationType.valueOf(conf.get(CREDENTIALS_TYPE));
      accountKind = AccountKind.valueOf(conf.get(MODE));
      account = Objects.requireNonNull(conf.get(ACCOUNT));
      secure = conf.getBoolean(SECURE, true);
      proto = accountKind.getPrototype(secure);
      conf.setIfUnset(AZURE_ENDPOINT, proto.getEndpointSuffix());
      azureEndpoint = conf.get(AZURE_ENDPOINT);
      final String connection = String.format("%s://%s.%s", proto.getEndpointScheme(), account, azureEndpoint);

      if (credentialsType == AZURE_ACTIVE_DIRECTORY) {
        clientID = Objects.requireNonNull(conf.get(CLIENT_ID));
        tokenEndpoint = Objects.requireNonNull(conf.get(TOKEN_ENDPOINT));
        clientSecret = Objects.requireNonNull(conf.get(CLIENT_SECRET));
        try {
          this.client = new DataLakeG2OAuthClient(account, new AzureTokenGenerator(tokenEndpoint, clientID, clientSecret), secure, azureEndpoint);
        } catch (IOException ioe) {
          throw ioe;
        } catch (Exception e){
          throw new IOException("Unable authenticate client with Azure Active Directory", e);
        }
      } else if(credentialsType == ACCESS_KEY ) {
        key = Objects.requireNonNull(conf.get(KEY));
        this.client = new DataLakeG2Client(account, key, secure, azureEndpoint);
      } else {
        throw new IOException("Unrecognized credential type");
      }

      final String[] containerList = getContainerNames(conf.get(CONTAINER_LIST));
      if(containerList != null) {
        blobProvider = new ProvidedContainerList(this, containerList);
      } else {
        if (accountKind == AccountKind.STORAGE_V2) {
          blobProvider = new FsV10Provider(client, this);
        } else {
          switch(credentialsType) {
            case ACCESS_KEY:
              blobProvider = new BlobContainerProvider(this, connection, account, key);
              break;
            case AZURE_ACTIVE_DIRECTORY:
              try {
                blobProvider = new BlobContainerProviderOAuth(this, connection, account,
                  new AzureTokenGenerator(tokenEndpoint, clientID, clientSecret));
              } catch(Exception e) {
                throw new IOException("Unable to establish connection to Storage V1 account with Azure Active Directory");
              }
              break;
            default:
              break;
          }
        }
      }

    } catch (InvalidKeyException e) {
      throw new IOException("Failure creating connection", e);
    }
  }

  private String[] getContainerNames(String value) {
    if(value == null) {
      return null;
    }

    String[] values = value.split(",");
    if(values.length == 0) {
      return null;
    }

    return values;
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {
    return blobProvider.getContainerCreators();
  }

  static final class ContainerCreatorImpl extends ContainerCreator {

    private final String container;
    private final AzureStorageFileSystem parent;

    public ContainerCreatorImpl(AzureStorageFileSystem parent, String container) {
      this.container = container;
      this.parent = parent;
    }

    @Override
    protected String getName() {
      return container;
    }

    @Override
    protected ContainerHolder toContainerHolder() throws IOException {
      return new ContainerHolder(container, new FileSystemSupplierImpl(container));
    }

    private final class FileSystemSupplierImpl extends FileSystemSupplier {

      private final String containerName;

      public FileSystemSupplierImpl(String containerName) {
        this.containerName = containerName;
      }

      @Override
      public FileSystem create() throws IOException {
        final Configuration conf = new Configuration(parent.parentConf);

        AzureAuthenticationType credentialsType = AzureAuthenticationType.valueOf(conf.get(CREDENTIALS_TYPE));
        final String location = parent.proto.getLocation(parent.account, containerName, parent.azureEndpoint);

        if (credentialsType == AZURE_ACTIVE_DIRECTORY) {
          parent.proto.setImpl(conf, parent.account, parent.clientID, parent.tokenEndpoint, parent.clientSecret, parent.azureEndpoint);
          return parent.fsCache.get(new Path(location).toUri(), conf, AzureStorageConf.AZURE_AD_PROPS);
        }

        parent.proto.setImpl(conf, parent.account, parent.key, parent.azureEndpoint);
        return parent.fsCache.get(new Path(location).toUri(), conf, AzureStorageConf.KEY_AUTH_PROPS);
      }
    }
  }

  DataLakeG2Client getClient() {
    return client;
  }

  @Override
  protected ContainerHolder getUnknownContainer(String name) throws IOException {
    // no lazy loading since it is slow.
    return null;
  }

  @Override
  public boolean supportsAsync() {
    return accountKind == AccountKind.STORAGE_V2;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version) throws IOException {
    return new AzureAsyncReader(path, client, version);
  }

  // Utility method to convert string representation of a datetime Long value into a HTTP 1.1
  // protocol compliant datetime string representation as follow:
  //      Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
  //      Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
  //      Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
  // References:
  // HTTP/1.1 protocol specification: https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3
  // Conditional headers: https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
  static String convertToHttpDateTime(String version) {
    ZonedDateTime date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(version)), ZoneId.of("GMT"));
    String dayOfWeek = date.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
    // DateTimeFormatter does not support commas
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss", Locale.ENGLISH);
    return String.format("%s, %s GMT", dayOfWeek, date.format(formatter));
  }

  private static final class AzureAsyncReader implements AsyncByteReader {

    private static final Logger logger = LoggerFactory.getLogger(AzureStoragePlugin.class);
    private final String container;
    private final String subpath;
    private final DataLakeG2Client client;
    private final String version;

    public AzureAsyncReader(Path path, DataLakeG2Client client, String version) {
      super();
      this.client = client;
      this.container = ContainerFileSystem.getContainerName(path);
      // Azure storage SDK adds a "/" between the container name and subpath name
      // Here we are using container utility functions built for S3 from the S3 plugin package
      // DX-15746: Refactoring needs to be done and there should be a container utility package that
      //           accommodate to cloud sources generally
      this.subpath = removeLeadingSlash(ContainerFileSystem.pathWithoutContainer(path).toString());
      this.version = convertToHttpDateTime(version);
    }

    @Override
    public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      dst.writerIndex(dstOffset);
      logger.debug("Reading data from container: {}", this.container);
      logger.debug("Reading data from container subpath: {}", this.subpath);
      try {
        client.read(container, subpath, offset, offset + len, version)
          .subscribe(

            // in single success
            response -> {
              Flowable<ByteBuffer> flowable = response.body();
              flowable.subscribe(

                // on flowable next
                bb -> {
                  dst.writeBytes(bb);
                },

                //on flowable exception
                ex -> {
                  logger.error("Error reading HTTP response asynchronously.", ex);
                  future.completeExceptionally(ex);
                },

                // on flowable complete
                () -> {
                  future.complete(null);
                }
              );

            },

            // on single failure
            ex -> {
              if (ex instanceof DataLakeStorageErrorException) {
                DataLakeStorageErrorException adlsEx = (DataLakeStorageErrorException) ex;
                if (adlsEx.body().error().code().equals("ConditionNotMet")) {
                  logger.debug("Version of file changed, metadata refresh is required");
                  future.completeExceptionally(new FileNotFoundException("Version of file changed " + subpath));
                } else {
                  logger.error("Error reading HTTP response asynchronously.", ex);
                  future.completeExceptionally(ex);
                }
              } else {
                logger.error("Error reading HTTP response asynchronously.", ex);
                future.completeExceptionally(ex);
              }
            });
      } catch (Exception ex) {
        logger.error("Unable to read with client: ", ex);
        future.completeExceptionally(ex);
      }
      return future;
    }
  }
}
