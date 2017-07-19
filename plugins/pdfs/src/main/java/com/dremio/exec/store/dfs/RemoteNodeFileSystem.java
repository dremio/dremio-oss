/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.dremio.exec.dfs.proto.DFS;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

/**
 * This filesystem is used to access remote local filesystem using SabotNode
 * custom RPC
 *
 * It is not intended to be used directly but in conjunction with
 * {@link PseudoDistributedFileSystem}
 *
 */
class RemoteNodeFileSystem extends FileSystem {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteNodeFileSystem.class);

  static final int REMOTE_WRITE_BUFFER_SIZE = 128*1024;
  private static final long RPC_TIMEOUT_MS = 5000;

  private static final Path ROOT_PATH = new Path("/");

  private static final class GetFileStatusCommand extends PDFSCommand<DFS.GetFileStatusResponse> {
    public GetFileStatusCommand(String path) {
      super(DFS.GetFileStatusResponse.class,
          DFS.RpcType.GET_FILE_STATUS_REQUEST,
          DFS.GetFileStatusRequest.newBuilder().setPath(path).build());
    }
  }

  private static final class GetFileDataCommand extends PDFSCommand<DFS.GetFileDataResponse> {
    public GetFileDataCommand(String path, long offset, int length) {
      super(DFS.GetFileDataResponse.class,
          DFS.RpcType.GET_FILE_DATA_REQUEST,
          DFS.GetFileDataRequest.newBuilder().setPath(path).setStart(offset).setLength(length).build());
    }
  }

  private static final class ListStatusCommand extends PDFSCommand<DFS.ListStatusResponse> {
    public ListStatusCommand(String path) {
      super(DFS.ListStatusResponse.class,
          DFS.RpcType.LIST_STATUS_REQUEST,
          DFS.ListStatusRequest.newBuilder().setPath(path).build());
    }
  }

  private static final class MkdirsCommand extends PDFSCommand<DFS.MkdirsResponse> {
    public MkdirsCommand(String path, Integer permission) {
      super(DFS.MkdirsResponse.class,
          DFS.RpcType.MKDIRS_REQUEST,
          newRequest(path, permission));
    }

    private static final DFS.MkdirsRequest newRequest(String path, Integer permission) {
      DFS.MkdirsRequest.Builder builder = DFS.MkdirsRequest.newBuilder().setPath(path);
      if (permission != null) {
        builder.setPermission(permission);
      }
      return builder.build();
    }
  }

  private static final class RenameCommand extends PDFSCommand<DFS.RenameResponse> {
    public RenameCommand(String oldPath, String newPath) {
      super(DFS.RenameResponse.class,
          DFS.RpcType.RENAME_REQUEST,
          DFS.RenameRequest.newBuilder().setOldpath(oldPath).setNewpath(newPath).build());
    }
  }

  private static final class DeleteCommand extends PDFSCommand<DFS.DeleteResponse> {
    public DeleteCommand(String path, boolean recursive) {
      super(DFS.DeleteResponse.class,
          DFS.RpcType.DELETE_REQUEST,
          DFS.DeleteRequest.newBuilder().setPath(path).setRecursive(recursive).build());
    }
  }

  private static class PDFSCommand<M extends MessageLite> extends FutureBitCommand<M, ProxyConnection> {
    private final Class<M> clazz;
    private final EnumLite rpcType;
    private final MessageLite request;

    protected PDFSCommand(Class<M> clazz, EnumLite rpcType, MessageLite request) {
      super();

      this.clazz = clazz;
      this.rpcType = rpcType;
      this.request = request;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<M> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, rpcType, request, clazz);
    }
  }

  private static SabotContext currentContext = null;

  /**
   * Register custom protocol for remote filesystem operations
   *
   * @param context
   *          the SabotContext instance to use
   * @throws IOException
   */
  static synchronized void registerProtocol(SabotContext context) throws IOException {
    Preconditions.checkNotNull(context);

    if (currentContext != context) {
      currentContext = context;
    }
  }

  /**
   * Converts a Hadoop {@link FileStatus} instance into a protobuf
   * {@link DFSProtos.FileStatus}
   *
   * @param status
   *          the Hadoop status instance to convert
   * @return a protobuf status instance
   * @throws IOException
   */
  static DFS.FileStatus toProtoFileStatus(FileStatus status) throws IOException {
    DFS.FileStatus.Builder builder = DFS.FileStatus.newBuilder();

    builder
      .setLength(status.getLen())
      .setIsDirectory(status.isDirectory())
      .setBlockReplication(status.getReplication())
      .setBlockSize(status.getBlockSize())
      .setModificationTime(status.getModificationTime())
      .setAccessTime(status.getAccessTime());

    // Handling potential null values
    if (status.getPath() != null) {
      builder = builder.setPath(status.getPath().toUri().getPath());
    }
    if (status.getPermission() != null) {
      builder = builder.setPermission(status.getPermission().toExtendedShort());
    }
    if (status.getOwner() != null) {
      builder = builder.setOwner(status.getOwner());
    }
    if (status.getGroup() != null) {
      builder = builder.setGroup(status.getGroup());
    }
    if (status.isSymlink()) {
      builder = builder.setSymlink(status.getSymlink().toString());
    }

    return builder.build();
  }

  static FsPermission toFsPermission(final Integer permissionValue) {
    return permissionValue != null ? FsPermission.createImmutable(permissionValue.shortValue()) : null;
  }


  /**
   * Converts a protobuf @link {@link DFS.FileStatus} instance into a
   * Hadoop {@link FileStatus}
   *
   * @param status
   *          the protobuf status instance to convert
   * @return the Hadoop status instance
   * @throws IOException
   */
  FileStatus fromProtoFileStatus(DFS.FileStatus status) throws IOException {
    final Integer permissionValue = status.getPermission();

    return new FileStatus(status.getLength(), status.getIsDirectory(), status.getBlockReplication(),
        status.getBlockSize(), status.getModificationTime(), status.getAccessTime(),
        toFsPermission(permissionValue),
        status.hasOwner() ? status.getOwner() : null,
        status.hasGroup() ? status.getGroup() : null,
        status.hasSymlink() ? new Path(status.getSymlink()) : null,
        status.hasPath() ? makeQualified(new Path(status.getPath())): null);
  }

  private final FabricCommandRunner runner;
  private final BufferAllocator allocator;

  private URI uri;
  private Path workingDirectory;

  public RemoteNodeFileSystem(FabricCommandRunner runner, BufferAllocator allocator) {
    this.runner = runner;
    this.workingDirectory = ROOT_PATH;
    this.allocator = allocator;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    if (name.getHost() == null || name.getPort() == -1) {
      throw new IllegalArgumentException("FileSystem name needs a complete authority element.");
    }
    uri = name;  }

  private Path toAbsolutePath(Path p) {
    if (p.isAbsolute()) {
      return p;
    }

    return new Path(workingDirectory, p);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  private static final ByteBuf EMPTY_BUFFER = Unpooled.unreleasableBuffer(Unpooled.EMPTY_BUFFER);

  private final class RemoteNodeInputStream extends FSInputStream {
    private final String path;
    private final int buffersize;

    private long pos = 0;
    private boolean closed = false;
    private boolean eof = false;
    private ByteBuf buf;
    private InputStream in;

    public RemoteNodeInputStream(String path, int buffersize) throws IOException {
      super();
      this.path = path;
      this.buffersize = buffersize;
      this.buf = EMPTY_BUFFER;
      this.in = new ByteBufInputStream(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      checkClosed();

      if (eof) {
        throw new EOFException("Stream is closed");
      }
      this.pos = pos;
      getData();
    }

    @Override
    public void close() throws IOException {
      if (this.closed) {
        return;
      }
      this.closed = true;

      super.close();

      in.close();
      buf.release();
    }

    @Override
    public long getPos() throws IOException {
      checkClosed();

      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      checkClosed();

      do {
        if (eof) {
          return -1;
        }
        int res = in.read();
        if (res != -1) {
          pos++;
          return res;
        }

        getData();
      } while(true);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      checkClosed();

      int read = 0;
      int res = 0;
      if (eof) {
        return -1;
      }
      do {
        if (eof) {
          return read;
        }
        res = in.read(b, off + read, len - read);
        if (res == -1) {
          getData();
        } else {
          read += res;
          pos += res;
        }
      } while(read < len);

      return read;
    }

    private void checkClosed() throws IOException {
      if (closed) {
        throw new IOException("Stream is closed");
      }
    }

    private void getData() throws IOException {
      // Free previous resources
      in.close();
      buf.release();

      final GetFileDataCommand command = new GetFileDataCommand(path, pos, buffersize);
      runner.runCommand(command);

      RpcFuture<DFS.GetFileDataResponse> future = command.getFuture();
      try {
        DFS.GetFileDataResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        eof = (response.getRead() == -1);
        buf = future.getBuffer();
        if (buf == null) {
          buf = EMPTY_BUFFER;
        }
        in = new ByteBufInputStream(buf);
      } catch(TimeoutException e) {
        throw new IOException("Timeout occured during I/O request for " + uri, e);
      } catch(RpcException e) {
        RpcException.propagateIfPossible(e, IOException.class);

        throw e;
      }
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Create a tunnel to connect remotely
    final String path = absolutePath.toUri().getPath();

    //return new InputStream
    return new FSDataInputStream(new RemoteNodeInputStream(path, bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    return new FSDataOutputStream(new LocalStatefulOutputStream(f.toString(), runner, allocator, REMOTE_WRITE_BUFFER_SIZE), null);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append is not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = toAbsolutePath(src);
    Path absoluteDst = toAbsolutePath(dst);
    checkPath(absoluteSrc);
    checkPath(absoluteDst);

    final RenameCommand command = new RenameCommand(absoluteSrc.toUri().getPath(), absoluteDst.toUri().getPath());
    runner.runCommand(command);

    RpcFuture<DFS.RenameResponse> future = command.getFuture();
    try {
      DFS.RenameResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return response.getValue();
    } catch(TimeoutException e) {
      throw new IOException("Timeout occured during I/O request for " + uri, e);
    } catch(RpcException e) {
      RpcException.propagateIfPossible(e, IOException.class);

      throw e;
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    final DeleteCommand command = new DeleteCommand(absolutePath.toUri().getPath(), recursive);
    runner.runCommand(command);

    RpcFuture<DFS.DeleteResponse> future = command.getFuture();
    try {
      DFS.DeleteResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return response.getValue();
    } catch(TimeoutException e) {
      throw new IOException("Timeout occured during I/O request for " + uri, e);
    } catch(RpcException e) {
      RpcException.propagateIfPossible(e, IOException.class);

      throw e;
    }
  }

  private static <T> List<T> getListOrEmpty(@Nullable List<T> list) {
    if (list == null) {
      return Collections.emptyList();
    }

    return list;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    final ListStatusCommand command = new ListStatusCommand(absolutePath.toUri().getPath());
    runner.runCommand(command);

    RpcFuture<DFS.ListStatusResponse> future = command.getFuture();
    try {
      DFS.ListStatusResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      List<DFS.FileStatus> protoStatuses = getListOrEmpty(response.getStatusesList());
      FileStatus[] statuses = new FileStatus[protoStatuses.size()];
      for (int i = 0; i < statuses.length; i++) {
        statuses[i] = fromProtoFileStatus(protoStatuses.get(i));
      }

      return statuses;
    } catch(TimeoutException e) {
      throw new IOException("Timeout occured during I/O request for " + uri, e);
    } catch(RpcException e) {
      RpcException.propagateIfPossible(e, IOException.class);

      throw e;
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    Path absolutePath = toAbsolutePath(newDir);
    checkPath(absolutePath);
    this.workingDirectory = absolutePath;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    final MkdirsCommand command = new MkdirsCommand(
        absolutePath.toUri().getPath(),
        permission != null ? (int) permission.toExtendedShort() : null);
    runner.runCommand(command);

    RpcFuture<DFS.MkdirsResponse> future = command.getFuture();
    try {
      DFS.MkdirsResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return response.getValue();
    } catch(TimeoutException e) {
      throw new IOException("Timeout occured during I/O request for " + uri, e);
    } catch(RpcException e) {
      RpcException.propagateIfPossible(e, IOException.class);

      throw e;
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    final GetFileStatusCommand command = new GetFileStatusCommand(absolutePath.toUri().getPath());
    runner.runCommand(command);

    RpcFuture<DFS.GetFileStatusResponse> future = command.getFuture();
    try {
      DFS.GetFileStatusResponse response = future.checkedGet(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return fromProtoFileStatus(response.getStatus());
    } catch(TimeoutException e) {
      throw new IOException("Timeout occured during I/O request for " + uri, e);
    } catch(RpcException e) {
      RpcException.propagateIfPossible(e, IOException.class);

      throw e;
    }
  }

}
