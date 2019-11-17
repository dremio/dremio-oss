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

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.dfs.proto.DFS;
import com.dremio.exec.dfs.proto.DFS.ListStatusContinuationHandle;
import com.dremio.exec.dfs.proto.DFS.WriteDataResponse;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.api.AbstractProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

final class PDFSProtocol extends AbstractProtocol {
  private static final ByteBuf[] NO_BUFS = new ByteBuf[] {};
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PDFSProtocol.class);

  public static final int PROTOCOL_ID = 42; // answer to the ultimate question
  public static final String OPEN_ITERATORS_TIMEOUT_MS_KEY = "dremio.pdfs.open-iterators.timeout";

  private final NodeEndpoint endpoint;
  private final BufferAllocator allocator;
  private final FileSystem localFS;
  private final boolean allowLocalAccess;

  private final int rpcTimeoutInSecs;

  private final Cache<ListStatusContinuationHandle, RemoteIterator<FileStatus>> openIterators;

  PDFSProtocol(NodeEndpoint endpoint, SabotConfig config, BufferAllocator allocator, FileSystem localFS,
      boolean allowLocalAccess, Ticker ticker) {
    this.endpoint = endpoint;
    this.allocator = allocator;
    this.localFS = localFS;
    this.allowLocalAccess = allowLocalAccess;

    this.rpcTimeoutInSecs = config.getInt(RpcConstants.BIT_RPC_TIMEOUT);

    long openIteratorsTimeoutMs = config.getMilliseconds(OPEN_ITERATORS_TIMEOUT_MS_KEY);
    this.openIterators = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterAccess(openIteratorsTimeoutMs, TimeUnit.MILLISECONDS)
        .removalListener(
            (RemovalNotification<ListStatusContinuationHandle, RemoteIterator<FileStatus>> notification) -> {
              if (notification.getCause() == RemovalCause.EXPLICIT) {
                return;
              }
              logger.info("Iterator for handle {} expired (cause: {})", notification.getKey(), notification.getCause());
              RemoteIterator<FileStatus> iterator = notification.getValue();
              if (iterator instanceof Closeable) {
                try {
                  ((Closeable) iterator).close();
                } catch (IOException e) {
                  // swallow exception
                  logger.warn("Exception thrown when closing iterator for handle {}", notification.getKey(), e);
                }
              }
            })
        .build();
  }

  /**
   * Create a new PDFS protocol instance.
   *
   * @param endpoint the local endpoint
   * @param config the Sabot configuration
   * @param allocator the memory allocator to use. The caller is in charge of closing it
   * @return the protocol
   * @throws IOException
   */
  public static PDFSProtocol newInstance(NodeEndpoint endpoint, SabotConfig config, BufferAllocator allocator,
      boolean allowLocalHandling) throws IOException {
    // we'll grab a raw local file system so append is supported (rather than
    // the checksum local file system).
    Configuration conf = new Configuration();
    return new PDFSProtocol(endpoint, config, allocator,
        PseudoDistributedFileSystem.newLocalFileSystem(conf, allowLocalHandling), allowLocalHandling,
        Ticker.systemTicker());
  }

  @Override
  public int getProtocolId() {
    return PROTOCOL_ID;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public RpcConfig getConfig() {
    return RpcConfig.newBuilder().name("pdfs")
        .timeout(rpcTimeoutInSecs)
        .add(DFS.RpcType.GET_FILE_STATUS_REQUEST, DFS.GetFileStatusRequest.class, DFS.RpcType.GET_FILE_STATUS_RESPONSE, DFS.GetFileStatusResponse.class)
        .add(DFS.RpcType.GET_FILE_DATA_REQUEST, DFS.GetFileDataRequest.class, DFS.RpcType.GET_FILE_DATA_RESPONSE, DFS.GetFileDataResponse.class)
        .add(DFS.RpcType.LIST_STATUS_REQUEST, DFS.ListStatusRequest.class, DFS.RpcType.LIST_STATUS_RESPONSE, DFS.ListStatusResponse.class)
        .add(DFS.RpcType.MKDIRS_REQUEST, DFS.MkdirsRequest.class, DFS.RpcType.MKDIRS_RESPONSE, DFS.MkdirsResponse.class)
        .add(DFS.RpcType.RENAME_REQUEST, DFS.RenameRequest.class, DFS.RpcType.RENAME_RESPONSE, DFS.RenameResponse.class)
        .add(DFS.RpcType.DELETE_REQUEST, DFS.DeleteRequest.class, DFS.RpcType.DELETE_RESPONSE, DFS.DeleteResponse.class)
        .add(DFS.RpcType.WRITE_DATA_REQUEST, DFS.WriteDataRequest.class, DFS.RpcType.WRITE_DATA_RESPONSE, DFS.WriteDataResponse.class)
        .build();
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch(rpcType) {
    case DFS.RpcType.GET_FILE_STATUS_RESPONSE_VALUE:
      return DFS.GetFileStatusResponse.getDefaultInstance();

    case DFS.RpcType.GET_FILE_DATA_RESPONSE_VALUE:
      return DFS.GetFileDataResponse.getDefaultInstance();

    case DFS.RpcType.LIST_STATUS_RESPONSE_VALUE:
      return DFS.ListStatusResponse.getDefaultInstance();

    case DFS.RpcType.MKDIRS_RESPONSE_VALUE:
      return DFS.MkdirsResponse.getDefaultInstance();

    case DFS.RpcType.RENAME_RESPONSE_VALUE:
      return DFS.RenameResponse.getDefaultInstance();

    case DFS.RpcType.DELETE_RESPONSE_VALUE:
      return DFS.DeleteResponse.getDefaultInstance();

    case DFS.RpcType.WRITE_DATA_RESPONSE_VALUE:
      return DFS.WriteDataResponse.getDefaultInstance();

    default:
      throw UserException
          .unsupportedError().message("Do not support response for rpc type %d", rpcType)
          .addIdentity(endpoint).build(logger);
    }
  }

  @Override
  protected Response handle(PhysicalConnection connection, int rpcType, ByteString pBody,
      ByteBuf dBody) throws RpcException {
    try {

      if(!allowLocalAccess){
        throw new RpcException(String.format("Attempted to access %s:%d which is a client-only node.",
          endpoint.getAddress(), endpoint.getFabricPort()));
      }

      switch(rpcType) {
      case DFS.RpcType.GET_FILE_STATUS_REQUEST_VALUE:
        return handle(connection, DFS.GetFileStatusRequest.parseFrom(pBody));

      case DFS.RpcType.GET_FILE_DATA_REQUEST_VALUE:
        return handle(connection, DFS.GetFileDataRequest.parseFrom(pBody));

      case DFS.RpcType.LIST_STATUS_REQUEST_VALUE:
        return handle(connection, DFS.ListStatusRequest.parseFrom(pBody));

      case DFS.RpcType.MKDIRS_REQUEST_VALUE:
        return handle(connection, DFS.MkdirsRequest.parseFrom(pBody));

      case DFS.RpcType.RENAME_REQUEST_VALUE:
        return handle(connection, DFS.RenameRequest.parseFrom(pBody));

      case DFS.RpcType.DELETE_REQUEST_VALUE:
        return handle(connection, DFS.DeleteRequest.parseFrom(pBody));

      case DFS.RpcType.WRITE_DATA_REQUEST_VALUE:
        return handle(connection, DFS.WriteDataRequest.parseFrom(pBody), dBody);

      default:
        throw UserException
          .unsupportedError().message("Do not support response for rpc type %d", rpcType)
          .addIdentity(endpoint)
          .build(logger);
      }
    } catch (InvalidProtocolBufferException e) {
      throw UserException.dataReadError(e).addIdentity(endpoint).build(logger);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).addIdentity(endpoint).build(logger);
    }
  }

  private Response handle(PhysicalConnection connection, DFS.GetFileStatusRequest request) throws IOException {
    Path path = new Path(request.getPath());

    FileStatus status = localFS.getFileStatus(path);
    DFS.GetFileStatusResponse response = DFS.GetFileStatusResponse.newBuilder()
        .setStatus(RemoteNodeFileSystem.toProtoFileStatus(status))
        .build();

    return reply(DFS.RpcType.GET_FILE_STATUS_RESPONSE, response);
  }

  private Response handle(PhysicalConnection connection, DFS.GetFileDataRequest request) throws IOException {
    Path path = new Path(request.getPath());

    try(FSDataInputStream fdis = localFS.open(path)) {
      byte[] buf = new byte[request.getLength()];

      fdis.seek(request.getStart());
      int read = fdis.read(buf);

      DFS.GetFileDataResponse response = DFS.GetFileDataResponse.newBuilder().setRead(read).build();
      ByteBuf[] bodies =  (read != -1) ? new ByteBuf[] { Unpooled.wrappedBuffer(buf, 0, read) } : NO_BUFS;

      return reply(DFS.RpcType.GET_FILE_DATA_RESPONSE, response, bodies);
    }
  }

  private Response handle(PhysicalConnection connection, DFS.WriteDataRequest request, ByteBuf buf) throws IOException {

    final Path path = new Path(request.getPath());
    if(request.getLastOffset() == 0){
      // initial creation and write.
      return writeData(path, buf, true);
    }

    // append, first check last update time and offset. (concurrency danger between check and write but doesn't
    // seem important in this usecase (home file uploads).)
    FileStatus fs = localFS.getFileStatus(path);
    if(fs.getModificationTime() != request.getLastUpdate()){
      throw new IOException(String.format("Unexpected last modification time. Expected time: %d, Actual time: %d.",
        request.getLastUpdate(), fs.getModificationTime()));
    }

    if(fs.getLen() != request.getLastOffset()) {
      throw new IOException(String.format("Unexpected last offset. Remote offset: %d, Actual offset: %d.",
        request.getLastOffset(), fs.getLen()));
    }

    return writeData(path, buf, false);

  }

  private Response writeData(Path path, ByteBuf buf, boolean create) throws IOException {
    try(FSDataOutputStream output = create ? localFS.create(path) : localFS.append(path)) {
      // empty buffers are just going to be null, we still want to create the file but write nothing to it
      if (buf != null) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        output.write(bytes);
      }
    }
    final FileStatus fs = localFS.getFileStatus(path);
    final WriteDataResponse response = WriteDataResponse.newBuilder().setUpdateTime(fs.getModificationTime()).build();
    return reply(DFS.RpcType.WRITE_DATA_RESPONSE, response);
  }

  private Response handle(PhysicalConnection connection, DFS.ListStatusRequest request) throws IOException {
    final RemoteIterator<FileStatus> iterator;
    if (request.hasHandle()) {
      final ListStatusContinuationHandle handle = request.getHandle();
      iterator = openIterators.getIfPresent(handle);
      if (iterator == null) {
        throw new IOException(format("No iterator found for handle %s/path %s. Maybe it expired?", handle, request.getPath()));
      }
      // invalidate the previous handle as a new one will be created if needed
      openIterators.invalidate(handle);
    } else {
      Preconditions.checkArgument(request.hasPath(), "No path argument provided for listStatus.");
      Path path = new Path(request.getPath());
      iterator = localFS.listStatusIterator(path);
    }

    final DFS.ListStatusResponse.Builder response = DFS.ListStatusResponse.newBuilder();
    try {
      // Only return as much as {limit} results (or all of them if no limit)
      for(int i = 0; iterator.hasNext() && (!request.hasLimit() || i < request.getLimit()); i++) {
        response.addStatuses(RemoteNodeFileSystem.toProtoFileStatus(iterator.next()));
      }

      // Check if more results are available
      if (iterator.hasNext()) {
        ListStatusContinuationHandle handle = ListStatusContinuationHandle.newBuilder()
            .setId(UUID.randomUUID().toString())
            .build();

        openIterators.put(handle, iterator);
        response.setHandle(handle);
      }
    } finally {
      // If response has no handle (because not enough results or exception)
      // make sure to close the iterator
      if (!response.hasHandle() && iterator instanceof Closeable) {
        ((Closeable) iterator).close();
      }
    }

    return reply(DFS.RpcType.LIST_STATUS_RESPONSE, response.build());
  }

  private Response handle(PhysicalConnection connection, DFS.MkdirsRequest request) throws IOException {
    Path path = new Path(request.getPath());

    FsPermission permission = RemoteNodeFileSystem.toFsPermission(request.getPermission());

    boolean result = localFS.mkdirs(path, permission);

    return reply(DFS.RpcType.MKDIRS_RESPONSE, DFS.MkdirsResponse.newBuilder().setValue(result).build());
  }

  private Response handle(PhysicalConnection connection, DFS.RenameRequest request) throws IOException {
    Path oldPath = new Path(request.getOldpath());
    Path newPath = new Path(request.getNewpath());

    boolean result = localFS.rename(oldPath, newPath);

    return reply(DFS.RpcType.RENAME_RESPONSE, DFS.RenameResponse.newBuilder().setValue(result).build());
  }

  private Response handle(PhysicalConnection connection, DFS.DeleteRequest request) throws IOException {
    Path path = new Path(request.getPath());

    Boolean recursive = request.getRecursive();
    boolean result = localFS.delete(path, recursive != null ? recursive.booleanValue() : false);

    return reply(DFS.RpcType.DELETE_RESPONSE, DFS.DeleteResponse.newBuilder().setValue(result).build());
  }

  private static Response reply(EnumLite rpcType, MessageLite msg, ByteBuf...bodies) {
    return new Response(rpcType, msg, bodies);
  }

  @VisibleForTesting
  boolean isIteratorOpen(ListStatusContinuationHandle handle) {
    return openIterators.getIfPresent(handle) != null;
  }
}
