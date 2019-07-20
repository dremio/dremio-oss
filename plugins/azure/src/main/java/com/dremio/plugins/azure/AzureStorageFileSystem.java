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

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.store.dfs.async.AsyncByteReader;
import com.dremio.plugins.azure.AzureStorageConf.AccountKind;
import com.dremio.plugins.util.ContainerFileSystem;

import io.netty.buffer.ByteBuf;
import io.reactivex.Flowable;

/**
 * A container file system implementation for Azure Storage.
 *
 * Supports both Blob containers and Hierarchical containers
 */
public class AzureStorageFileSystem extends ContainerFileSystem implements AsyncByteReader.MayProvideAsyncStream {

  private static final Logger logger = LoggerFactory.getLogger(AzureStorageFileSystem.class);

  static final String SCHEME = "dremioAzureStorage://";
  private static final String CONTAINER_HUMAN_NAME = "Container";
  static final String ACCOUNT = "dremio.azure.account";
  static final String KEY = "dremio.azure.key";
  static final String MODE = "dremio.azure.mode";
  static final String SECURE = "dremio.azure.secure";
  static final String CONTAINER_LIST = "dremio.azure.container_list";
  static final String AZURE_ENDPOINT = "fs.azure.endpoint";

  private String azureEndpoint;
  private String account;
  private String key;
  private boolean secure;
  private AccountKind type;
  private Prototype proto;
  private Configuration parentConf;
  private ContainerProvider blobProvider;
  private DataLakeG2Client client;

  protected AzureStorageFileSystem() {
    super(SCHEME, CONTAINER_HUMAN_NAME, a -> true);
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    parentConf = conf;
    try {
      account = Objects.requireNonNull(conf.get(ACCOUNT));
      key = Objects.requireNonNull(conf.get(KEY));
      secure = conf.getBoolean(SECURE, true);
      type = AccountKind.valueOf(conf.get(MODE));
      proto = type.getPrototype(secure);
      conf.setIfUnset(AZURE_ENDPOINT, proto.getEndpointSuffix());
      azureEndpoint = conf.get(AZURE_ENDPOINT);
      final String connection = String.format("%s://%s.%s", proto.getEndpointScheme(), account, azureEndpoint);

      this.client = new DataLakeG2Client(account, key, secure, azureEndpoint);
      final String[] containerList = getContainerNames(conf.get(CONTAINER_LIST));
      if(containerList != null) {
        blobProvider = new ProvidedContainerList(this, containerList);
      } else {
        if (type == AccountKind.STORAGE_V2) {
          blobProvider = new FsV10Provider(client, this);
        } else {
          blobProvider = new BlobContainerProvider(this, connection, account, key);
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
        try {
          final Configuration conf = new Configuration(parent.parentConf);
          parent.proto.setImpl(conf, parent.account, containerName, parent.key, parent.azureEndpoint);
          return FileSystem.get(conf);
        } catch(RuntimeException | IOException e) {
          throw e;
        }
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
    return type == AccountKind.STORAGE_V2;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path) throws IOException {
    return new AzureAsyncReader(path, client);
  }

  private static final class AzureAsyncReader implements AsyncByteReader {

    private static final Logger logger = LoggerFactory.getLogger(AzureStoragePlugin.class);
    private final String container;
    private final String subpath;
    private final DataLakeG2Client client;

    public AzureAsyncReader(Path path, DataLakeG2Client client) {
      super();
      this.client = client;
      this.container = ContainerFileSystem.getContainerName(path);
      // Azure storage SDK adds a "/" between the container name and subpath name
      // Here we are using container utility functions built for S3 from the S3 plugin package
      // DX-15746: Refactoring needs to be done and there should be a container utility package that
      //           accommodate to cloud sources generally
      this.subpath = removeLeadingSlash(ContainerFileSystem.pathWithoutContainer(path).toString());
    }

    @Override
    public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      dst.writerIndex(dstOffset);
      logger.debug("Reading data from container: {}", this.container);
      logger.debug("Reading data from container subpath: {}", this.subpath);
      client.read(container, subpath, offset, offset + len)
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
            logger.error("Error reading HTTP response asynchronously.", ex);
            future.completeExceptionally(ex);
          });
      return future;
    }
  }
}
