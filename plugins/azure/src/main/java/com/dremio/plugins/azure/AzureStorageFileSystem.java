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
import static com.dremio.common.utils.PathUtils.removeTrailingSlash;
import static com.dremio.plugins.azure.AzureAuthenticationType.ACCESS_KEY;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.Retryer;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.http.AsyncHttpClientProvider;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.azure.AbstractAzureStorageConf.AccountKind;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container file system implementation for Azure Storage.
 *
 * <p>Supports both Blob containers and Hierarchical containers
 */
public class AzureStorageFileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
  private static final Logger logger = LoggerFactory.getLogger(AzureStorageFileSystem.class);
  private static final String CONTAINER_HUMAN_NAME = "Container";
  public static final String ACCOUNT = "dremio.azure.account";
  public static final String KEY = "dremio.azure.key";
  public static final String MODE = "dremio.azure.mode";
  public static final String SECURE = "dremio.azure.secure";
  static final String CONTAINER_LIST = "dremio.azure.container_list";
  public static final String ROOT_PATH = "dremio.azure.rootPath";

  public static final String CREDENTIALS_TYPE = "dremio.azure.credentialsType";

  public static final String CLIENT_ID = "dremio.azure.clientId";
  public static final String TOKEN_ENDPOINT = "dremio.azure.tokenEndpoint";
  public static final String CLIENT_SECRET = "dremio.azure.clientSecret";
  static final String AZURE_ENDPOINT = "fs.azure.endpoint";

  public static final String AZURE_SHAREDKEY_SIGNER_TYPE = "fs.azure.sharedkey.signer.type";

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();

  private Configuration parentConf;
  private AccountKind accountKind;
  private boolean secure;
  private Prototype proto;

  private String azureEndpoint;
  private String account;

  private AsyncHttpClient asyncHttpClient;
  private ContainerProvider containerProvider;

  private AzureAuthTokenProvider tokenProvider;

  private boolean enableMD5Checksum;

  @Override
  public void close() throws IOException {
    AutoCloseables.close(IOException.class, () -> fsCache.closeAll(true), super::close);
  }

  protected AzureStorageFileSystem() {
    super(
        FileSystemConf.CloudFileSystemScheme.AZURE_STORAGE_FILE_SYSTEM_SCHEME.getScheme(),
        CONTAINER_HUMAN_NAME,
        a -> true);
  }

  @Override
  public long getTTL(com.dremio.io.file.FileSystem fileSystem, com.dremio.io.file.Path path) {
    // TODO : fetch TTL for AzureStorageFileSystem
    logger.error("Fetching TTL for AzureStorageFS is unavailable.");
    return -1;
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    parentConf = conf;
    conf.setIfUnset(CREDENTIALS_TYPE, ACCESS_KEY.name());
    AzureAuthenticationType credentialsType =
        AzureAuthenticationType.valueOf(conf.get(CREDENTIALS_TYPE));

    // TODO: check if following are needed
    accountKind = AccountKind.valueOf(conf.get(MODE));
    secure = conf.getBoolean(SECURE, true);
    proto = accountKind.getPrototype(secure);
    conf.setIfUnset(AZURE_ENDPOINT, proto.getEndpointSuffix());
    azureEndpoint = conf.get(AZURE_ENDPOINT);
    // -- End --

    account = Objects.requireNonNull(conf.get(ACCOUNT));
    asyncHttpClient = AsyncHttpClientProvider.getInstance();
    enableMD5Checksum = conf.getBoolean(AzureStorageOptions.ENABLE_CHECKSUM.getOptionName(), true);

    switch (credentialsType) {
      case AZURE_ACTIVE_DIRECTORY:
        {
          final ClientCredentialsBasedTokenProvider tokenProvider =
              new ClientCredentialsBasedTokenProviderImpl();
          tokenProvider.initialize(conf, account);

          this.tokenProvider = tokenProvider;
          break;
        }

      case ACCESS_KEY:
        {
          conf.setIfUnset(AZURE_SHAREDKEY_SIGNER_TYPE, AzureSharedKeyCredentials.class.getName());
          final AzureSharedKeyCredentials sharedKeyCredentials = new AzureSharedKeyCredentials();
          sharedKeyCredentials.setConf(conf);

          this.tokenProvider = sharedKeyCredentials;
          break;
        }

      default:
        throw new IOException("Unrecognized credential type");
    }

    final String[] containerList;
    String rootPath = conf.get(ROOT_PATH);
    if (rootPath != null) {
      rootPath = removeLeadingSlash(rootPath);
      rootPath = removeTrailingSlash(rootPath);
    }
    if (rootPath != null && rootPath.length() > 0) {
      containerList = getContainerNameFromRootPath(rootPath).split(" ");
      rootPath = getPathWithoutContainer(rootPath);
    } else {
      containerList = getContainerNames(conf.get(CONTAINER_LIST));
      rootPath = null;
    }

    switch (accountKind) {
      case STORAGE_V2:
        containerProvider =
            new AzureAsyncContainerProvider(
                asyncHttpClient,
                azureEndpoint,
                account,
                tokenProvider,
                this,
                secure,
                containerList,
                rootPath);
        break;

      case STORAGE_V1:
        final URI connection;
        try {
          connection = new URI(proto.getConnection(account, azureEndpoint));
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Invalid URI", e);
        }

        switch (credentialsType) {
          case ACCESS_KEY:
            containerProvider =
                new BlobContainerProviderUsingKey(
                    this,
                    connection,
                    account,
                    containerList,
                    ((AzureStorageCredentials) tokenProvider));
            break;

          case AZURE_ACTIVE_DIRECTORY:
            containerProvider =
                new BlobContainerProviderUsingOAuth(
                    this,
                    connection,
                    account,
                    containerList,
                    (ClientCredentialsBasedTokenProvider) tokenProvider);
            break;

          default:
            throw new IllegalStateException("Unrecognized credential type: " + credentialsType);
        }
        break;

      default:
        throw new IllegalStateException("Unrecognized account kind: " + accountKind);
    }

    containerProvider.verfiyContainersExist();
  }

  private String[] getContainerNames(String value) {
    if (value == null) {
      return null;
    }

    String[] values = value.split(",");
    if (values.length == 0) {
      return null;
    }

    return values;
  }

  private String getContainerNameFromRootPath(String path) {
    final String[] pathComponents = path.split(Path.SEPARATOR);
    if (pathComponents.length == 0) {
      return null;
    }
    return pathComponents[0];
  }

  private String getPathWithoutContainer(String path) {
    String[] pathComponents = path.split(Path.SEPARATOR);
    if (pathComponents.length == 1) {
      return null;
    }
    return Joiner.on(Path.SEPARATOR)
        .join(Arrays.copyOfRange(pathComponents, 1, pathComponents.length));
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {
    return containerProvider.getContainerCreators();
  }

  static final class ContainerCreatorImpl extends ContainerCreator {

    private final String container;
    private final AzureStorageFileSystem parent;

    public ContainerCreatorImpl(AzureStorageFileSystem parent, String container) {
      this.container = container;
      this.parent = parent;
    }

    @VisibleForTesting
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

        final String location =
            parent.proto.getLocation(parent.account, containerName, parent.azureEndpoint);

        final AzureAuthenticationType credentialsType =
            AzureAuthenticationType.valueOf(conf.get(CREDENTIALS_TYPE));
        switch (credentialsType) {
          case AZURE_ACTIVE_DIRECTORY:
            // TODO(DX-68107): STORAGE_V1
            final String tokenEndpoint = Objects.requireNonNull(conf.get(TOKEN_ENDPOINT));
            final String clientID = Objects.requireNonNull(conf.get(CLIENT_ID));
            final String clientSecret =
                new String(Objects.requireNonNull(conf.getPassword(CLIENT_SECRET)));
            parent.proto.setImpl(
                conf, parent.account, clientID, tokenEndpoint, clientSecret, parent.azureEndpoint);
            return parent.fsCache.get(
                new Path(location).toUri(), conf, AbstractAzureStorageConf.AZURE_AD_PROPS);

          case ACCESS_KEY:
            final String key = new String(Objects.requireNonNull(conf.getPassword(KEY)));
            parent.proto.setImpl(conf, parent.account, key, parent.azureEndpoint);
            return parent.fsCache.get(
                new Path(location).toUri(), conf, AbstractAzureStorageConf.KEY_AUTH_PROPS);

          default:
            throw new IllegalStateException("Unrecognized credential type: " + credentialsType);
        }
      }
    }
  }

  @Override
  protected ContainerHolder getUnknownContainer(String containerName) throws IOException {
    try {
      containerProvider.assertContainerExists(containerName);
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      throw e.getWrappedCause(IOException.class, ex -> new IOException(ex));
    }
    return new ContainerCreatorImpl(this, containerName).toContainerHolder();
  }

  @Override
  public boolean supportsAsync() {
    return accountKind == AccountKind.STORAGE_V2;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(
      Path path, String version, Map<String, String> options) {
    return new AzureAsyncReader(
        azureEndpoint,
        account,
        path,
        tokenProvider,
        version,
        secure,
        asyncHttpClient,
        enableMD5Checksum);
  }
}
