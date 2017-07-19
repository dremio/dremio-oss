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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.dfs.proto.DFS;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.rpc.Response;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.dremio.test.DremioTest;

import io.netty.buffer.ByteBuf;

/**
 * Test class for {@link PDFSProtocol}
 */
public abstract class TestPDFSProtocol extends ExecTest {

  private static final NodeEndpoint LOCAL_ENDPOINT = newNodeEndpoint("10.0.0.1", 1234);
  private static final String TEST_PATH_STRING = "/tmp/foo";
  private static final Path TEST_PATH = new Path(TEST_PATH_STRING);
  private static final FileStatus TEST_FILE_STATUS = new FileStatus(1024, false, 1, 4096, 1453325757, 1453325758, new FsPermission((short) 0644), "testowner",
      "testgroup", TEST_PATH);

  private static final String TEST_PATH_2_STRING = "/tmp/foo2";
  private static final Path TEST_PATH_2 = new Path(TEST_PATH_2_STRING);


  /**
   * Test for class {@link PDFSProtocol} getFileStatus operations
   */
  public static class TestGetFileStatusHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private DFS.GetFileStatusResponse getResponse(final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).getFileStatus(TEST_PATH);
      } else {
        doReturn(o).when(getFileSystem()).getFileStatus(TEST_PATH);
      }


      Response response = getPDFSProtocol().handle(getConnection(), DFS.RpcType.GET_FILE_STATUS_REQUEST_VALUE,
          DFS.GetFileStatusRequest.newBuilder().setPath(TEST_PATH_STRING).build().toByteString(), null);

      assertEquals(DFS.RpcType.GET_FILE_STATUS_RESPONSE, response.rpcType);
      assertArrayEquals(new ByteBuf[] {}, response.dBodies);
      return (DFS.GetFileStatusResponse) response.pBody;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      DFS.GetFileStatusResponse response = getResponse(TEST_FILE_STATUS);

      assertEquals(TEST_PATH_STRING, response.getStatus().getPath());
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(new FileNotFoundException("Where is the file?"));
        fail("Expected UserException/FileNoFoundExpection");
      } catch(UserException e) {
        // Expected
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(new IOException());
        fail("Expected UserException/IOException");
      } catch(UserException e) {
        // Expected
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
      }
    }
  }

  /**
   * Test for class {@link PDFSProtocol} get file data operations
   */
  public static class TestGetFileDataHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private Response getResponse(Long start, Integer length, final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).open(TEST_PATH);
      } else {
        doReturn(o).when(getFileSystem()).open(TEST_PATH);
      }

      Response response = getPDFSProtocol().handle(getConnection(),
          DFS.RpcType.GET_FILE_DATA_REQUEST_VALUE,
          DFS.GetFileDataRequest.newBuilder().setPath(TEST_PATH_STRING).setStart(start).setLength(length).build().toByteString(),
          null);

      assertEquals(DFS.RpcType.GET_FILE_DATA_RESPONSE, response.rpcType);
      //assertArrayEquals(new ByteBuf[] {}, response.dBodies);
      return response;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      InputStream mis = mock(InputStream.class, withSettings().extraInterfaces(Seekable.class, PositionedReadable.class));
      doReturn(42).when(mis).read(any(byte[].class), anyInt(), anyInt());

      FSDataInputStream fdis = new FSDataInputStream(mis);
      Response response = getResponse(7L, 4096, fdis);

      InOrder inOrder = Mockito.inOrder(mis);

      inOrder.verify((Seekable) mis).seek(7);
      inOrder.verify(mis).read(any(byte[].class), anyInt(), anyInt());

      assertEquals(42, ((DFS.GetFileDataResponse) response.pBody).getRead());
      assertEquals(42, response.dBodies[0].readableBytes());
    }

    @Test
    public void testOnMessageEOF() throws IOException {
      InputStream mis = mock(InputStream.class, withSettings().extraInterfaces(Seekable.class, PositionedReadable.class));
      doReturn(-1).when(mis).read(any(byte[].class), anyInt(), anyInt());

      FSDataInputStream fdis = new FSDataInputStream(mis);
      Response response = getResponse(7L, 4096, fdis);

      InOrder inOrder = Mockito.inOrder(mis);

      inOrder.verify((Seekable) mis).seek(7);
      inOrder.verify(mis).read(any(byte[].class), anyInt(), anyInt());

      assertEquals(-1, ((DFS.GetFileDataResponse) response.pBody).getRead());
      assertEquals(0, response.dBodies.length);
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(0L, 4096, new FileNotFoundException("Where is the file?"));
        fail("Was expecting UserException/FileNotFoundException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(0L, 4096, new IOException("Something happened"));
        fail("Was expecting UserException/IOException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
        assertEquals("Something happened", e.getCause().getMessage());
      }
    }
  }

  /**
   * Test class for {@link PDFSProtocol} listStatus operation
   */
  public static class TestListFileStatusHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private DFS.ListStatusResponse getResponse(final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).listStatus(TEST_PATH);
      } else {
        doReturn(o).when(getFileSystem()).listStatus(TEST_PATH);
      }

      Response response = getPDFSProtocol().handle(getConnection(),
          DFS.RpcType.LIST_STATUS_REQUEST_VALUE,
          DFS.ListStatusRequest.newBuilder().setPath(TEST_PATH_STRING).build().toByteString(),
          null);

      assertEquals(DFS.RpcType.LIST_STATUS_RESPONSE, response.rpcType);
      assertArrayEquals(new ByteBuf[] {}, response.dBodies);

      return (DFS.ListStatusResponse) response.pBody;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      FileStatus[] statuses = {
          new FileStatus(1337, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", new Path(TEST_PATH, "bar")),
          new FileStatus(0, true, 0, 0, 3, 4, FsPermission.getDirDefault(), "testowner", "testgroup", new Path(TEST_PATH, "baz"))
      };

      DFS.ListStatusResponse response = getResponse(statuses);

      assertEquals(2, response.getStatusesList().size());
      assertEquals(TEST_PATH_STRING + "/bar", response.getStatusesList().get(0).getPath());
      assertEquals(TEST_PATH_STRING + "/baz", response.getStatusesList().get(1).getPath());
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(new FileNotFoundException("Where is the file?"));
        fail("Was expecting UserException/FileNotFoundException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(new IOException());
        fail("Was expecting UserException/IOException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
      }
    }
  }

  /**
   * Test for class {@link PDFSProtocol} delete operations
   */
  public static class TestDeleteHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private DFS.DeleteResponse getResponse(final boolean recursive, final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).delete(TEST_PATH, recursive);
      } else {
        doReturn(o).when(getFileSystem()).delete(TEST_PATH, recursive);
      }

      Response response = getPDFSProtocol().handle(getConnection(),
          DFS.RpcType.DELETE_REQUEST_VALUE,
          DFS.DeleteRequest.newBuilder().setPath(TEST_PATH_STRING).setRecursive(recursive).build().toByteString(),
          null);

      assertEquals(DFS.RpcType.DELETE_RESPONSE, response.rpcType);
      assertArrayEquals(new ByteBuf[] {}, response.dBodies);

      return (DFS.DeleteResponse) response.pBody;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      DFS.DeleteResponse response = getResponse(false, true);

      assertEquals(true, response.getValue());
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(true, new FileNotFoundException("Where is the file?"));
        fail("Was expecting UserException/FileNotFoundException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(true, new IOException());
        fail("Was expecting UserException/IOException");
      } catch (UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
      }
    }
  }


  /**
   * Test for class {@link PDFSProtocol} mkdirs operations
   */
  public static class TestMkdirsHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private DFS.MkdirsResponse getResponse(final FsPermission permission, final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).mkdirs(TEST_PATH, permission);
      } else {
        doReturn(o).when(getFileSystem()).mkdirs(TEST_PATH, permission);
      }

      Response response = getPDFSProtocol().handle(getConnection(),
          DFS.RpcType.MKDIRS_REQUEST_VALUE,
          DFS.MkdirsRequest.newBuilder().setPath(TEST_PATH_STRING).setPermission(permission.toExtendedShort()).build().toByteString(),
          null);

      assertEquals(DFS.RpcType.MKDIRS_RESPONSE, response.rpcType);
      assertArrayEquals(new ByteBuf[] {}, response.dBodies);

      return (DFS.MkdirsResponse) response.pBody;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      DFS.MkdirsResponse response = getResponse(FsPermission.getDirDefault(), true);

      assertEquals(true, response.getValue());
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(FsPermission.getDirDefault(), new FileNotFoundException("Where is the file?"));
        fail("Was expecting UserException/FileNotFoundException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(FsPermission.getDirDefault(), new IOException());
        fail("Was expecting UserException/IOException");
      } catch (UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
      }

    }
  }

  /**
   * Test for class {@link PDFSProtocol} rename operations
   */
  public static class TestRenameHandler extends TestPDFSProtocol {
    /**
     * Get the response produced by the handler for a given filesystem response.
     *
     * @return
     * @throws IOException
     * @throws UserException
     */
    private DFS.RenameResponse getResponse(final Object o) throws IOException, UserException {
      if (o instanceof Throwable) {
        doThrow((Throwable) o).when(getFileSystem()).rename(TEST_PATH, TEST_PATH_2);
      } else {
        doReturn(o).when(getFileSystem()).rename(TEST_PATH, TEST_PATH_2);
      }

      Response response = getPDFSProtocol().handle(getConnection(),
          DFS.RpcType.RENAME_REQUEST_VALUE,
          DFS.RenameRequest.newBuilder().setOldpath(TEST_PATH_STRING).setNewpath(TEST_PATH_2_STRING).build().toByteString(),
          null);

      assertEquals(DFS.RpcType.RENAME_RESPONSE, response.rpcType);
      assertArrayEquals(new ByteBuf[] {}, response.dBodies);

      return (DFS.RenameResponse) response.pBody;
    }

    @Test
    public void testOnMessageSuccessful() throws IOException {
      DFS.RenameResponse response = getResponse(true);

      assertEquals(true, response.getValue());
    }

    @Test
    public void testOnMessageFileNotFound() throws IOException {
      try {
        getResponse(new FileNotFoundException("Where is the file?"));
        fail("Was expecting UserException/FileNotFoundException");
      } catch(UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(FileNotFoundException.class, e.getCause().getClass());
        assertEquals("Where is the file?", e.getCause().getMessage());
      }
    }

    @Test
    public void testOnMessageIOException() throws IOException {
      try {
        getResponse(new IOException());
        fail("Was expecting UserException/IOException");
      } catch (UserException e) {
        assertEquals(ErrorType.IO_EXCEPTION, e.getErrorType());
        assertSame(IOException.class, e.getCause().getClass());
      }
    }
  }

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  @Mock private FileSystem fileSystem;
  @Mock private PhysicalConnection connection;

  private PDFSProtocol pdfsProtocol;

  @Before
  public void setUp() throws IOException {
    pdfsProtocol = new PDFSProtocol(LOCAL_ENDPOINT, DremioTest.DEFAULT_SABOT_CONFIG, this.allocator, fileSystem, true);
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

  protected PDFSProtocol getPDFSProtocol() {
    return pdfsProtocol;
  }

  protected FileSystem getFileSystem() {
    return fileSystem;
  }

  protected PhysicalConnection getConnection() {
    return connection;
  }
}
