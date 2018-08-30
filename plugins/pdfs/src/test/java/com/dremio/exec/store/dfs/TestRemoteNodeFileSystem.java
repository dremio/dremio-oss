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
package com.dremio.exec.store.dfs;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.dfs.proto.DFS;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcCommand;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test class for {@link RemoteNodeFileSystem}
 */
public class TestRemoteNodeFileSystem extends ExecTest {
  private static final int TEST_RPC_TIMEOUT_MS = 100;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRemoteNodeFileSystem.class);

  private static final NodeEndpoint REMOTE_ENDPOINT = newNodeEndpoint("10.0.0.2", 1234);
  private static final NodeEndpoint LOCAL_ENDPOINT = newNodeEndpoint("10.0.0.1", 1234);
  private static final List<NodeEndpoint> ENDPOINTS_LIST = Arrays.asList(LOCAL_ENDPOINT, REMOTE_ENDPOINT);

  private static final String TEST_PATH_STRING = "/tmp/foo";
  private static final Path TEST_PATH = new Path(TEST_PATH_STRING);
  private static final FileStatus TEST_FILE_STATUS = new FileStatus(1024, false, 1, 4096, 1453325757, 1453325758, new FsPermission((short) 0644), "testowner",
      "testgroup", TEST_PATH);

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  @Mock private FabricCommandRunner runner;

  @Test
  public void testToProtobuFileStatus() throws IOException {
    FileStatus status = TEST_FILE_STATUS;

    DFS.FileStatus result = RemoteNodeFileSystem.toProtoFileStatus(status);
    assertEquals(TEST_PATH_STRING, result.getPath());
    assertEquals(1024,result.getLength());
    assertFalse(result.getIsDirectory());
    assertEquals(1, result.getBlockReplication());
    assertEquals(4096, result.getBlockSize());
    assertEquals(1453325758, result.getAccessTime());
    assertEquals(1453325757, result.getModificationTime());
    assertEquals(0644, result.getPermission());
    assertEquals("testowner", result.getOwner());
    assertEquals("testgroup", result.getGroup());
    assertFalse(result.hasSymlink());

  }

  @Test
  public void testToProtobuFileStatusWithDefault() throws IOException {
    FileStatus status = new FileStatus();

    DFS.FileStatus result = RemoteNodeFileSystem.toProtoFileStatus(status);
    assertFalse(result.hasPath());
    assertEquals(0, result.getLength());
    assertFalse(result.getIsDirectory());
    assertEquals(0, result.getBlockReplication());
    assertEquals(0, result.getBlockSize());
    assertEquals(0, result.getAccessTime());
    assertEquals(0, result.getModificationTime());
    assertEquals(FsPermission.getFileDefault().toExtendedShort(), result.getPermission());
    assertEquals("", result.getOwner());
    assertEquals("", result.getGroup());
    assertFalse(result.hasSymlink());
  }

  @Test
  public void testToProtobuFileStatusWithDirectory() throws IOException {
    FileStatus status = new FileStatus(0, true, 0, 0, 1, 2, null, null, null, TEST_PATH);

    DFS.FileStatus result = RemoteNodeFileSystem.toProtoFileStatus(status);
    assertEquals(TEST_PATH_STRING, result.getPath());
    assertEquals(0, result.getLength());
    assertTrue(result.getIsDirectory());
    assertEquals(0, result.getBlockReplication());
    assertEquals(0, result.getBlockSize());
    assertEquals(2, result.getAccessTime());
    assertEquals(1, result.getModificationTime());
    assertEquals(FsPermission.getDirDefault().toExtendedShort(), result.getPermission());
    assertEquals("", result.getOwner());
    assertEquals("", result.getGroup());
    assertFalse(result.hasSymlink());
  }

  /**
   * Creates a pseudo distributed filesystem with the provided context
   *
   * @param context
   * @return
   * @throws IOException
   */
  private RemoteNodeFileSystem newRemoteNodeFileSystem() throws IOException {
    final Configuration configuration = new Configuration(false);
    configuration.setTimeDuration(RemoteNodeFileSystem.RPC_TIMEOUT_KEY, TEST_RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return newRemoteNodeFileSystem(configuration);
  }

  private RemoteNodeFileSystem newRemoteNodeFileSystem(Configuration configuration) throws IOException {
    RemoteNodeFileSystem fs =  new RemoteNodeFileSystem(runner, allocator);
    fs.initialize(URI.create(String.format("sabot://%s:%d/", REMOTE_ENDPOINT.getAddress(), REMOTE_ENDPOINT.getFabricPort())), configuration);
    return fs;
  }

  /**
   * Create a {@link com.dremio.exec.dfs.proto.beans.NodeEndpoint}
   * @param address the address
   * @param port the control port
   * @return
   */
  private static NodeEndpoint newNodeEndpoint(String address, int port) {
    return NodeEndpoint.newBuilder().setAddress(address).setFabricPort(port).build();
  }

  private void setupRPC(final EnumLite requestType, final MessageLite request, final EnumLite responseType, final MessageLite response) throws Exception {
    setupRPC(requestType, request, responseType, response, null);
  }

  private void setupRPC(final EnumLite requestType, final MessageLite request, final EnumLite responseType, final MessageLite response, final ByteBuf buffer) throws Exception {
    setupRPC(requestType, Arrays.asList(request), responseType, Arrays.asList(response), Arrays.asList(buffer));
  }

  @SuppressWarnings("unchecked")
  private void setupRPC(final EnumLite requestType, final List<MessageLite> requests, final EnumLite responseType, final List<MessageLite> responses, final List<ByteBuf> buffers) throws Exception {
    final ProxyConnection proxyConnection = mock(ProxyConnection.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new AssertionError(format("Unexpected call to ProxyConnection#send with parameters: %s", Arrays.toString(invocation.getArguments())));
      }
    }).when(proxyConnection).send(any(RpcOutcomeListener.class), any(EnumLite.class), any(MessageLite.class), any(Class.class));

    for(int i = 0; i < requests.size(); i++) {
      MessageLite request = requests.get(i);
      MessageLite response = responses.get(i);
      ByteBuf buffer = buffers.get(i);

      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(final InvocationOnMock invocation) throws Throwable {
          final RpcOutcomeListener<MessageLite> listener = invocation.getArgumentAt(0, RpcOutcomeListener.class);
          listener.success(response, buffer);

          return null;
        }
      }).when(proxyConnection).send(any(RpcOutcomeListener.class), eq(requestType), eq(request), eq(response.getClass()));
    }

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new AssertionError(format("Unexpected call to FabricCommandRunner#runCommand with parameters: %s", Arrays.toString(invocation.getArguments())));
      }
    }).when(runner).runCommand(any(RpcCommand.class));

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        final RpcCommand<?, ProxyConnection> rpcCommand = invocation.getArgumentAt(0, RpcCommand.class);
        rpcCommand.connectionSucceeded(proxyConnection);
        return null;
      }
    }).when(runner).runCommand(any(RpcCommand.class));
  }



  @SuppressWarnings("unchecked")
  private void setupRPC(final EnumLite requestType, final MessageLite request, Class<? extends MessageLite> responseClazz, final RpcException e) throws Exception {
    final ProxyConnection proxyConnection = mock(ProxyConnection.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new AssertionError(format("Unexpected call to ProxyConnection#send with parameters: %s", Arrays.toString(invocation.getArguments())));
      }
    }).when(proxyConnection).send(any(RpcOutcomeListener.class), any(EnumLite.class), any(MessageLite.class), any(Class.class));

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        final RpcOutcomeListener<MessageLite> listener = invocation.getArgumentAt(0, RpcOutcomeListener.class);
        listener.failed(e);

        return null;
      }
    }).when(proxyConnection).send(any(RpcOutcomeListener.class), eq(requestType), eq(request), eq(responseClazz));

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new AssertionError(format("Unexpected call to FabricCommandRunner#runCommand with parameters: %s", Arrays.toString(invocation.getArguments())));
      }
    }).when(runner).runCommand(any(RpcCommand.class));

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        final RpcCommand<?, ProxyConnection> rpcCommand = invocation.getArgumentAt(0, RpcCommand.class);
        rpcCommand.connectionSucceeded(proxyConnection);
        return null;
      }
    }).when(runner).runCommand(any(RpcCommand.class));

  }

  @Test
  public void testGetFileStatusRoot() throws Exception {
    {
      DFS.FileStatus status = newFileStatus(1024L, true, 0, 4096L, 37L, 42L, 0755, "root", "wheel", "/");
      setupRPC(
          DFS.RpcType.GET_FILE_STATUS_REQUEST, DFS.GetFileStatusRequest.newBuilder().setPath("/").build(),
          DFS.RpcType.GET_FILE_STATUS_RESPONSE, DFS.GetFileStatusResponse.newBuilder().setStatus(status).build());
    }

    FileSystem fs = newRemoteNodeFileSystem();

    Path root = new Path("/");
    FileStatus status = fs.getFileStatus(root);

    assertEquals(new Path("sabot://10.0.0.2:1234/"), status.getPath());
    assertTrue(status.isDirectory());
    assertEquals(0755, status.getPermission().toExtendedShort());
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetFileStatusWithInvalidPath() throws Exception {
    {
      setupRPC(
          DFS.RpcType.GET_FILE_STATUS_REQUEST, DFS.GetFileStatusRequest.newBuilder().setPath("/foo/bar").build(),
          DFS.GetFileStatusResponse.class, newRPCException(LOCAL_ENDPOINT, new FileNotFoundException("File not found")));
    }

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    fs.getFileStatus(path);
  }

  private static DFS.FileStatus newFileStatus(long length, boolean isDir, int blockReplication, long blockSize, long accessTime, long modificationTime, int permission, String owner, String group, String path) {
    DFS.FileStatus status = DFS.FileStatus.newBuilder()
        .setLength(length)
        .setIsDirectory(isDir)
        .setBlockReplication(blockReplication)
        .setBlockSize(blockSize)
        .setModificationTime(modificationTime)
        .setAccessTime(accessTime)
        .setPermission(permission)
        .setOwner(owner)
        .setGroup(group)
        .setPath(path)
        .build();

    return status;
  }

  @Test
  public void testGetFileStatusWithValidPath() throws Exception {
    {
      DFS.FileStatus status = newFileStatus(1024, false, 1, 4096, 42, 37, 0644 , "root", "wheel", "/foo/bar");
      setupRPC(
          DFS.RpcType.GET_FILE_STATUS_REQUEST, DFS.GetFileStatusRequest.newBuilder().setPath("/foo/bar").build(),
          DFS.RpcType.GET_FILE_STATUS_RESPONSE, DFS.GetFileStatusResponse.newBuilder().setStatus(status).build());
    }

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    FileStatus status = fs.getFileStatus(path);

    // getFileStatus returns qualified path
    assertEquals(new Path("sabot://10.0.0.2:1234/foo/bar"), status.getPath());
    assertFalse(status.isDirectory());
    assertEquals(1024, status.getLen());
    assertEquals(1, status.getReplication());
    assertEquals(4096, status.getBlockSize());
    assertEquals(42, status.getAccessTime());
    assertEquals(37, status.getModificationTime());
    assertEquals("root", status.getOwner());
    assertEquals("wheel", status.getGroup());
    assertEquals(0644, status.getPermission().toExtendedShort());
  }

  @Test
  public void testListStatusRoot() throws Exception {
    {
      DFS.ListStatusResponse listStatusResponse = DFS.ListStatusResponse.newBuilder()
          .addStatuses(newFileStatus(42, true, 0, 0, 456, 879, 0755, "root", "wheel", "/foo"))
          .addStatuses(newFileStatus(1024, false, 1, 4096, 354, 435, 0644, "admin", "admin", "/bar"))
          .build();

      setupRPC(
          DFS.RpcType.LIST_STATUS_REQUEST, DFS.ListStatusRequest.newBuilder().setPath("/").setLimit(RemoteNodeFileSystem.LIST_STATUS_BATCH_SIZE_DEFAULT).build(),
          DFS.RpcType.LIST_STATUS_RESPONSE, listStatusResponse);
    }

    FileSystem fs = newRemoteNodeFileSystem();

    Path root = new Path("/");
    FileStatus[] statuses = fs.listStatus(root);

    assertEquals(ENDPOINTS_LIST.size(), statuses.length);

    assertEquals(new Path("sabot://10.0.0.2:1234/foo"), statuses[0].getPath());
    assertTrue(statuses[0].isDirectory());
    assertEquals(0755, statuses[0].getPermission().toExtendedShort());

    assertEquals(new Path("sabot://10.0.0.2:1234/bar"), statuses[1].getPath());
    assertFalse(statuses[1].isDirectory());
    assertEquals(0644, statuses[1].getPermission().toExtendedShort());
  }

  @Test
  public void testListStatusRootOneByOne() throws Exception {
    {
      DFS.ListStatusContinuationHandle handle = DFS.ListStatusContinuationHandle.newBuilder().setId("test-handle").build();

      DFS.ListStatusResponse listStatusInitialResponse = DFS.ListStatusResponse.newBuilder()
          .addStatuses(newFileStatus(42, true, 0, 0, 456, 879, 0755, "root", "wheel", "/foo"))
          .setHandle(handle)
          .build();
      DFS.ListStatusResponse listStatusLastResponse = DFS.ListStatusResponse.newBuilder()
          .addStatuses(newFileStatus(1024, false, 1, 4096, 354, 435, 0644, "admin", "admin", "/bar"))
          .build();

      setupRPC(
          DFS.RpcType.LIST_STATUS_REQUEST,
          Arrays.asList(
              DFS.ListStatusRequest.newBuilder().setPath("/").setLimit(1).build(),
              DFS.ListStatusRequest.newBuilder().setPath("/").setHandle(handle).setLimit(1).build()),
          DFS.RpcType.LIST_STATUS_RESPONSE,
          Arrays.asList(listStatusInitialResponse, listStatusLastResponse),
          Arrays.asList((ByteBuf) null, null)
          );
    }
    // Limit the number of results per batch
    final Configuration configuration = new Configuration(false);
    configuration.setTimeDuration(RemoteNodeFileSystem.RPC_TIMEOUT_KEY, TEST_RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    configuration.setInt(RemoteNodeFileSystem.LIST_STATUS_BATCH_SIZE_KEY, 1);

    FileSystem fs = newRemoteNodeFileSystem(configuration);

    Path root = new Path("/");
    FileStatus[] statuses = fs.listStatus(root);

    assertEquals(ENDPOINTS_LIST.size(), statuses.length);

    assertEquals(new Path("sabot://10.0.0.2:1234/foo"), statuses[0].getPath());
    assertTrue(statuses[0].isDirectory());
    assertEquals(0755, statuses[0].getPermission().toExtendedShort());

    assertEquals(new Path("sabot://10.0.0.2:1234/bar"), statuses[1].getPath());
    assertFalse(statuses[1].isDirectory());
    assertEquals(0644, statuses[1].getPermission().toExtendedShort());
  }

  @Test
  public void testListStatusWithValidPath() throws Exception {
    {
      DFS.ListStatusResponse listStatusResponse = DFS.ListStatusResponse.newBuilder()
          .addStatuses(newFileStatus(42, true, 0, 0, 456, 879, 0755, "root", "wheel", "/foo/bar/dir"))
          .addStatuses(newFileStatus(1024, false, 1, 4096, 354, 435, 0644, "admin", "admin", "/foo/bar/file"))
          .build();

      setupRPC(
          DFS.RpcType.LIST_STATUS_REQUEST, DFS.ListStatusRequest.newBuilder().setPath("/foo/bar").setLimit(RemoteNodeFileSystem.LIST_STATUS_BATCH_SIZE_DEFAULT).build(),
          DFS.RpcType.LIST_STATUS_RESPONSE, listStatusResponse);
    }

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    FileStatus[] statuses = fs.listStatus(path);

    assertEquals(ENDPOINTS_LIST.size(), statuses.length);

    assertEquals(new Path("sabot://10.0.0.2:1234/foo/bar/dir"), statuses[0].getPath());
    assertTrue(statuses[0].isDirectory());
    assertEquals(0755, statuses[0].getPermission().toExtendedShort());

    assertEquals(new Path("sabot://10.0.0.2:1234/foo/bar/file"), statuses[1].getPath());
    assertFalse(statuses[1].isDirectory());
    assertEquals(0644, statuses[1].getPermission().toExtendedShort());
  }

  @Test(expected = FileNotFoundException.class)
  public void testListStatusWithInvalidPath() throws Exception {
    setupRPC(
        DFS.RpcType.LIST_STATUS_REQUEST, DFS.ListStatusRequest.newBuilder().setPath("/foo/bar").setLimit(RemoteNodeFileSystem.LIST_STATUS_BATCH_SIZE_DEFAULT).build(),
        DFS.ListStatusResponse.class, newRPCException(LOCAL_ENDPOINT, new FileNotFoundException("File not found")));

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    fs.listStatus(path);
  }

  @Test
  public void testDeleteWithValidPath() throws Exception {
    setupRPC(
        DFS.RpcType.DELETE_REQUEST, DFS.DeleteRequest.newBuilder().setPath("/foo/bar").setRecursive(true).build(),
        DFS.RpcType.DELETE_RESPONSE, DFS.DeleteResponse.newBuilder().setValue(true).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    boolean result = fs.delete(path, true);

    assertTrue(result);
  }

  @Test
  public void testDeleteWithValidPathButNotDeleted() throws Exception {
    setupRPC(
        DFS.RpcType.DELETE_REQUEST, DFS.DeleteRequest.newBuilder().setPath("/foo/bar").setRecursive(true).build(),
        DFS.RpcType.DELETE_RESPONSE, DFS.DeleteResponse.newBuilder().setValue(false).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    boolean result = fs.delete(path, true);

    assertFalse(result);
  }

  private static final RpcException newRPCException(NodeEndpoint endpoint, IOException ioe) {
    UserRemoteException ure = UserRemoteException.create(UserException
        .ioExceptionError(ioe)
        .addIdentity(endpoint)
        .build(logger).getOrCreatePBError(false));

    return new RpcException(ure);
  }
  @Test
  public void testDeleteWithInvalidPath() throws Exception {
    setupRPC(
        DFS.RpcType.DELETE_REQUEST, DFS.DeleteRequest.newBuilder().setPath("/foo/bar").setRecursive(true).build(),
        DFS.DeleteResponse.class, newRPCException(LOCAL_ENDPOINT, new FileNotFoundException("File not found")));

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    try {
      fs.delete(path, true);
      fail("Expected fs.delete() to throw FileNotFoundException");
    } catch(FileNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void testMkdirsWithValidPath() throws Exception {
    setupRPC(
        DFS.RpcType.MKDIRS_REQUEST, DFS.MkdirsRequest.newBuilder().setPath("/foo/bar").setPermission(0755).build(),
        DFS.RpcType.MKDIRS_RESPONSE, DFS.MkdirsResponse.newBuilder().setValue(true).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    boolean result = fs.mkdirs(path, FsPermission.createImmutable((short) 0755));

    assertTrue(result);
  }

  @Test
  public void testMkdirsWithValidPathButNotCreated() throws Exception {
    setupRPC(
        DFS.RpcType.MKDIRS_REQUEST, DFS.MkdirsRequest.newBuilder().setPath("/foo/bar").setPermission(0755).build(),
        DFS.RpcType.MKDIRS_RESPONSE, DFS.MkdirsResponse.newBuilder().setValue(false).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    boolean result = fs.mkdirs(path, FsPermission.createImmutable((short) 0755));

    assertFalse(result);
  }

  @Test
  public void testMkdirsWithInvalidPath() throws Exception {
    setupRPC(
        DFS.RpcType.MKDIRS_REQUEST, DFS.MkdirsRequest.newBuilder().setPath("/foo/bar").setPermission(0755).build(),
        DFS.MkdirsResponse.class, newRPCException(LOCAL_ENDPOINT, new FileNotFoundException("File not found")));

    FileSystem fs = newRemoteNodeFileSystem();

    Path path = new Path("/foo/bar");
    try {
      fs.mkdirs(path, FsPermission.createImmutable((short) 0755));
      fail("Expected fs.mkdirs() to throw FileNotFoundException");
    } catch(FileNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void testRenameWithValidPath() throws Exception {
    setupRPC(
        DFS.RpcType.RENAME_REQUEST, DFS.RenameRequest.newBuilder().setOldpath("/foo/bar").setNewpath("/foo/bar2").build(),
        DFS.RpcType.RENAME_RESPONSE, DFS.RenameResponse.newBuilder().setValue(true).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path oldPath = new Path("/foo/bar");
    Path newPath = new Path("/foo/bar2");
    boolean result = fs.rename(oldPath, newPath);

    assertTrue(result);
  }

  @Test
  public void testRenameWithButNotRenamed() throws Exception {
    setupRPC(
        DFS.RpcType.RENAME_REQUEST, DFS.RenameRequest.newBuilder().setOldpath("/foo/bar").setNewpath("/foo/bar2").build(),
        DFS.RpcType.RENAME_RESPONSE, DFS.RenameResponse.newBuilder().setValue(false).build());

    FileSystem fs = newRemoteNodeFileSystem();

    Path oldPath = new Path("/foo/bar");
    Path newPath = new Path("/foo/bar2");
    boolean result = fs.rename(oldPath, newPath);

    assertFalse(result);
  }

  @Test
  public void testRenameWithInvalidPath() throws Exception {
    setupRPC(
        DFS.RpcType.RENAME_REQUEST, DFS.RenameRequest.newBuilder().setOldpath("/foo/bar").setNewpath("/foo/bar2").build(),
        DFS.RenameResponse.class, newRPCException(LOCAL_ENDPOINT, new FileNotFoundException("File not found")));

    FileSystem fs = newRemoteNodeFileSystem();

    Path oldPath = new Path("/foo/bar");
    Path newPath = new Path("/foo/bar2");

    try {
      fs.rename(oldPath, newPath);
      fail("Expected fs.mkdirs() to throw FileNotFoundException");
    } catch(FileNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void testInputStream() throws Exception {
    Path filePath = new Path("/foo/bar");
    byte[] data = new byte[100];
    for (int i = 0; i < 100; ++i) {
      data[i] = (byte)i;
    }
    byte [] readBuf = new byte[1000];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, 100);

    setupRPC(
      DFS.RpcType.GET_FILE_DATA_REQUEST, DFS.GetFileDataRequest.newBuilder().setPath(filePath.toString()).setStart(0).setLength(100).build(),
      DFS.RpcType.GET_FILE_DATA_RESPONSE, DFS.GetFileDataResponse.newBuilder().setRead(100).build(), byteBuf);

    FileSystem fs = newRemoteNodeFileSystem();
    FSDataInputStream inputStream = fs.open(filePath, 100);
    int read = inputStream.read(readBuf, 0, 50); // read first 50 bytes to trigger first rpc
    assertEquals(50, read);

    setupRPC(
      DFS.RpcType.GET_FILE_DATA_REQUEST, DFS.GetFileDataRequest.newBuilder().setPath(filePath.toString()).setStart(100).setLength(100).build(),
      DFS.RpcType.GET_FILE_DATA_RESPONSE, DFS.GetFileDataResponse.newBuilder().setRead(-1).build());

    read = inputStream.read(readBuf, 50, 1000); // read rest of the data
    assertEquals(50, read);
    for (int i = 0; i < 100; ++i) {
      assertEquals((byte)i, readBuf[i]);
    }
  }
}
