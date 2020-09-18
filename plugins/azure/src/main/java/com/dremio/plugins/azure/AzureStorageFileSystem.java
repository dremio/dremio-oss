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

import static com.dremio.plugins.azure.AzureAuthenticationType.ACCESS_KEY;
import static com.dremio.plugins.azure.AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.Retryer;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.azure.AzureStorageConf.AccountKind;
import com.dremio.plugins.azure.utils.AsyncHttpClientProvider;
import com.dremio.plugins.util.ContainerFileSystem;

/**
 * A container file system implementation for Azure Storage.
 * <p>
 * Supports both Blob containers and Hierarchical containers
 */
public class AzureStorageFileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
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
  private String key = null;

  private AzureAuthenticationType credentialsType;

  private String clientID;
  private String tokenEndpoint;
  private String clientSecret;

  private boolean secure;
  private AccountKind accountKind;
  private Prototype proto;
  private Configuration parentConf;
  private ContainerProvider containerProvider;
  private AzureAuthTokenProvider authProvider;
  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();


  private AsyncHttpClient asyncHttpClient;

  @Override
  public void close() throws IOException {
    AutoCloseables.close(IOException.class,
      () -> fsCache.closeAll(true),
      super::close);
  }

  protected AzureStorageFileSystem() {
    super(FileSystemConf.CloudFileSystemScheme.AZURE_STORAGE_FILE_SYSTEM_SCHEME.getScheme(), CONTAINER_HUMAN_NAME, a -> true);
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    parentConf = conf;
    conf.setIfUnset(CREDENTIALS_TYPE, ACCESS_KEY.name());
    credentialsType = AzureAuthenticationType.valueOf(conf.get(CREDENTIALS_TYPE));

    // TODO: check if following are needed
    accountKind = AccountKind.valueOf(conf.get(MODE));
    secure = conf.getBoolean(SECURE, true);
    proto = accountKind.getPrototype(secure);
    conf.setIfUnset(AZURE_ENDPOINT, proto.getEndpointSuffix());
    azureEndpoint = conf.get(AZURE_ENDPOINT);
    // -- End --

    account = Objects.requireNonNull(conf.get(ACCOUNT));
    asyncHttpClient = AsyncHttpClientProvider.getInstance();

    if (credentialsType == AZURE_ACTIVE_DIRECTORY) {
      clientID = Objects.requireNonNull(conf.get(CLIENT_ID));
      tokenEndpoint = Objects.requireNonNull(conf.get(TOKEN_ENDPOINT));
      clientSecret = Objects.requireNonNull(conf.get(CLIENT_SECRET));
      this.authProvider = new AzureOAuthTokenProvider(tokenEndpoint, clientID, clientSecret);
    } else if (credentialsType == ACCESS_KEY) {
      key = Objects.requireNonNull(conf.get(KEY));
      this.authProvider = new AzureSharedKeyAuthTokenProvider(account, key);
    } else {
      throw new IOException("Unrecognized credential type");
    }

    final String[] containerList = getContainerNames(conf.get(CONTAINER_LIST));
    if (containerList != null) {
      containerProvider = new ProvidedContainerList(this, containerList);
    } else {
      if (accountKind == AccountKind.STORAGE_V2) {
        containerProvider = new AzureAsyncContainerProvider(asyncHttpClient, account, authProvider, this, secure);
      } else {
        final String connection = String.format("%s://%s.%s", proto.getEndpointScheme(), account, azureEndpoint);
        switch (credentialsType) {
          case ACCESS_KEY:
            containerProvider = new BlobContainerProvider(this, connection, account, key);
            break;
          case AZURE_ACTIVE_DIRECTORY:
            try {
              containerProvider = new BlobContainerProviderOAuth(this, connection, account, (AzureOAuthTokenProvider) authProvider);
            } catch (Exception e) {
              throw new IOException("Unable to establish connection to Storage V1 account with Azure Active Directory");
            }
            break;
          default:
            break;
        }
      }
    }
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
  public AsyncByteReader getAsyncByteReader(Path path, String version) {
    return new AzureAsyncReader(account, path, authProvider, version, secure, asyncHttpClient);
  }
}
