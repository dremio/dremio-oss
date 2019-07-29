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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.dremio.exec.ExecTest;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.CoordinationProtos.Roles;
import com.dremio.service.DirectProvider;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Test class for {@link PseudoDistributedFileSystem}
 *
 */
public class TestPseudoDistributedFileSystem extends ExecTest {
  private static final NodeEndpoint REMOTE_ENDPOINT_1 = newNodeEndpoint("10.0.0.2", 1234);
  private static final NodeEndpoint REMOTE_ENDPOINT_2 = newNodeEndpoint("10.0.0.2", 5678);
  private static final NodeEndpoint REMOTE_ENDPOINT_3 = newNodeEndpoint("10.0.0.1", 9012);
  private static final NodeEndpoint LOCAL_ENDPOINT = newNodeEndpoint("10.0.0.1", 1234);
  private static final Provider<Iterable<NodeEndpoint>> ENDPOINTS_PROVIDER =
      DirectProvider.<Iterable<NodeEndpoint>>wrap((Arrays.asList(LOCAL_ENDPOINT, REMOTE_ENDPOINT_1, REMOTE_ENDPOINT_2, REMOTE_ENDPOINT_3)));

  private Configuration hadoopConf;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private FileSystem mockLocalFS;
  @Mock private FileSystem mockRemoteFS;


  private PseudoDistributedFileSystem fs;

  private static class MockRemoteStatusIterator implements RemoteIterator<FileStatus> {
    MockRemoteStatusIterator(FileStatus[] statuses) {
      this.statuses = Arrays.asList(statuses).iterator();
    }

    private final Iterator<FileStatus> statuses;

    @Override
    public boolean hasNext() throws IOException {
      return statuses.hasNext();
    }

    @Override
    public FileStatus next() throws IOException {
      return statuses.next();
    }
  }

  @Before
  public void setUp() throws IOException {
    PDFSConfig config = new PDFSConfig(
        MoreExecutors.newDirectExecutorService(),
        null,
        null,
        ENDPOINTS_PROVIDER,
        LOCAL_ENDPOINT,
        true);
    hadoopConf = new Configuration();
    fs = newPseudoDistributedFileSystem(config);
  }

  @Before
  public void setUpLocalFS() throws IOException {
    final FileStatus rootStatus = new FileStatus(4096, true, 0, 0, 37, 42, FsPermission.createImmutable((short) 0555), "root", "wheel", new Path("sabot://10.0.0.1:1234/"));
    final FileStatus fooStatus = new FileStatus(38214, true, 0, 0, 45, 67, FsPermission.createImmutable((short) 0755), "root", "wheel", new Path("sabot://10.0.0.1:1234/foo"));
    final FileStatus fooBarStatus = new FileStatus(67128, true, 1, 4096, 69, 68, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.1:1234/foo/bar"));
    final FileStatus fooBarDirStatus = new FileStatus(47, true, 0, 0, 1234, 3645, FsPermission.createImmutable((short) 0755), "admin", "admin", new Path("sabot://10.0.0.1:1234/foo/bar/dir"));
    final FileStatus fooBarFile1Status = new FileStatus(1024, false, 1, 4096, 37, 42, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.1:1234/foo/bar/file1"));
    final FileStatus fooBarFile2Status = new FileStatus(2048, false, 1, 4096, 37, 42, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.1:1234/foo/bar/file2"));

    doReturn(rootStatus).when(mockLocalFS).getFileStatus(new Path("/"));
    doThrow(new FileNotFoundException()).when(mockLocalFS).getFileStatus(any(Path.class));
    doReturn(fooBarFile2Status).when(mockLocalFS).getFileStatus(new Path("/foo/bar/file2"));
    doReturn(fooBarFile1Status).when(mockLocalFS).getFileStatus(new Path("/foo/bar/file1"));
    doReturn(fooBarDirStatus).when(mockLocalFS).getFileStatus(new Path("/foo/bar/dir"));
    doReturn(fooBarStatus).when(mockLocalFS).getFileStatus(new Path("/foo/bar"));
    doReturn(fooStatus).when(mockLocalFS).getFileStatus(new Path("/foo"));
    doReturn(rootStatus).when(mockLocalFS).getFileStatus(new Path("/"));

    final FileStatus[] fooBarStatusList = new FileStatus[] { fooBarDirStatus, fooBarFile1Status, fooBarFile2Status };
    final FileStatus[] fooStatusList = new FileStatus[] { fooBarStatus };
    final FileStatus[] rootStatusList = new FileStatus[] { fooStatus };

    // listStatusIterator mocks.
    doThrow(new FileNotFoundException()).when(mockLocalFS).listStatusIterator(any(Path.class));
    doReturn(new MockRemoteStatusIterator(fooBarStatusList)).when(mockLocalFS).listStatusIterator(new Path("/foo/bar"));
    doReturn(new MockRemoteStatusIterator(fooStatusList)).when(mockLocalFS).listStatusIterator(new Path("/foo"));
    doReturn(new MockRemoteStatusIterator(rootStatusList)).when(mockLocalFS).listStatusIterator(new Path("/"));

    // listStatus mocks.
    doThrow(new FileNotFoundException()).when(mockLocalFS).listStatus(any(Path.class));
    doReturn(fooBarStatusList).when(mockLocalFS).listStatus(new Path("/foo/bar"));
    doReturn(fooStatusList).when(mockLocalFS).listStatus(new Path("/foo"));
    doReturn(rootStatusList).when(mockLocalFS).listStatus(new Path("/"));
  }

  @Before
  public void setUpRemoteFS() throws IOException {
    final FileStatus rootStatus = new FileStatus(4096, true, 0, 0, 38, 43, FsPermission.createImmutable((short) 0555), "root", "wheel", new Path("sabot://10.0.0.2:1234/"));
    final FileStatus fooStatus = new FileStatus(37126, true, 0, 0, 41, 87, FsPermission.createImmutable((short) 0755), "root", "wheel", new Path("sabot://10.0.0.2:1234/foo"));
    final FileStatus fooBarStatus = new FileStatus(67128, true, 1, 4096, 54, 90, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.2:1234/foo/bar"));
    final FileStatus fooBarDirStatus = new FileStatus(47, true, 0, 0, 1234, 3645, FsPermission.createImmutable((short) 0755), "admin", "admin", new Path("sabot://10.0.0.2:1234/foo/bar/dir"));
    final FileStatus fooBarFile1Status = new FileStatus(1027, false, 1, 4096, 37, 42, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.2:1234/foo/bar/file1"));
    final FileStatus fooBarFile2Status = new FileStatus(2049, false, 1, 4096, 37, 42, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.2:1234/foo/bar/file3"));

    doThrow(new FileNotFoundException()).when(mockRemoteFS).getFileStatus(any(Path.class));
    doReturn(fooBarFile2Status).when(mockRemoteFS).getFileStatus(new Path("/foo/bar/file2"));
    doReturn(fooBarFile1Status).when(mockRemoteFS).getFileStatus(new Path("/foo/bar/file1"));
    doReturn(fooBarDirStatus).when(mockRemoteFS).getFileStatus(new Path("/foo/bar/dir"));
    doReturn(fooBarStatus).when(mockRemoteFS).getFileStatus(new Path("/foo/bar"));
    doReturn(fooStatus).when(mockRemoteFS).getFileStatus(new Path("/foo"));
    doReturn(rootStatus).when(mockRemoteFS).getFileStatus(new Path("/"));

    final FileStatus[] fooBarStatusList = new FileStatus[] { fooBarDirStatus, fooBarFile1Status, fooBarFile2Status };
    final FileStatus[] fooStatusList = new FileStatus[] { fooBarStatus };
    final FileStatus[] rootStatusList = new FileStatus[] { fooStatus };

    // listStatusIterator mocks.
    doThrow(new FileNotFoundException()).when(mockRemoteFS).listStatusIterator(any(Path.class));
    doReturn(new MockRemoteStatusIterator(fooBarStatusList)).when(mockRemoteFS).listStatusIterator(new Path("/foo/bar"));
    doReturn(new MockRemoteStatusIterator(fooStatusList)).when(mockRemoteFS).listStatusIterator(new Path("/foo"));
    doReturn(new MockRemoteStatusIterator(rootStatusList)).when(mockRemoteFS).listStatusIterator(new Path("/"));

    // listStatus mocks.
    doThrow(new FileNotFoundException()).when(mockRemoteFS).listStatus(any(Path.class));
    doReturn(fooBarStatusList).when(mockRemoteFS).listStatus(new Path("/foo/bar"));
    doReturn(fooStatusList).when(mockRemoteFS).listStatus(new Path("/foo"));
    doReturn(rootStatusList).when(mockRemoteFS).listStatus(new Path("/"));
  }

  /**
   * Creates a pseudo distributed filesystem with the provided context
   *
   * @param context
   * @return
   * @throws IOException
   */
  private PseudoDistributedFileSystem newPseudoDistributedFileSystem(PDFSConfig config) throws IOException {
    PseudoDistributedFileSystem fs =  new PseudoDistributedFileSystem(config) {
      @Override
      FileSystem newRemoteFileSystem(NodeEndpoint endpoint) throws IOException {
        assertEquals("10.0.0.2", endpoint.getAddress());

        return mockRemoteFS;
      }
    };
    fs.setConf(hadoopConf);
    fs.initialize(URI.create("pdfs:///"), hadoopConf, mockLocalFS);
    return fs;
  }

  /**
   * Create a {@link com.dremio.exec.proto.beans.NodeEndpoint}
   * @param address the address
   * @param port the control port
   * @return
   */
  private static NodeEndpoint newNodeEndpoint(String address, int port) {
    return NodeEndpoint.newBuilder().setAddress(address).setFabricPort(port).setRoles(Roles.newBuilder().setJavaExecutor(true)).build();
  }

  @Test
  public void testGetFileStatusRoot() throws IOException {
    Path root = new Path("/");
    FileStatus status = fs.getFileStatus(root);

    assertEquals(new Path("pdfs:/"), status.getPath());
    assertTrue(status.isDirectory());
    assertEquals(0555, status.getPermission().toExtendedShort());
  }

  @Test
  public void testGetFileStatusForInvalidPath() throws IOException {
    Path path = new Path("/foo/baz");
    try {
      fs.getFileStatus(path);
      fail("Expected getFileStatus to throw FileNotFoundException");
    } catch (FileNotFoundException e) {
    }
  }

  @Test
  public void testGetFileStatusForValidDirectory() throws IOException {
    Path path = new Path("/foo/bar");
    FileStatus status = fs.getFileStatus(path);
    assertEquals(new Path("pdfs:/foo/bar"), status.getPath());
    assertTrue(status.isDirectory());
    assertEquals(69, status.getModificationTime());
    assertEquals(90, status.getAccessTime());
  }

  @Test
  public void testGetFileStatusForUnaccessibleDirectory() throws IOException {
    doThrow(new AccessControlException()).when(mockRemoteFS).getFileStatus(new Path("/foo/baz"));

    Path path = new Path("/foo/baz");
    try {
      fs.getFileStatus(path);
      fail("Expected some IOException");
    } catch(AccessControlException e) {
      // ignore
    }
  }

  @Test
  public void testGetFileStatusWithValidRemotePath() throws IOException {
    doReturn(new FileStatus(1024, false, 1, 4096, 37, 42, FsPermission.createImmutable((short) 0644), "root", "wheel", new Path("sabot://10.0.0.2:1234/tmp/file")))
    .when(mockRemoteFS).getFileStatus(new Path("/tmp/file"));

    Path path = new Path("/tmp/10.0.0.2@file");
    FileStatus status = fs.getFileStatus(path);

    // getFileStatus returns qualified path
    assertEquals(new Path("pdfs:/tmp/10.0.0.2@file"), status.getPath());
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
  public void testGetFileStatusWithValidLocalPath() throws IOException {
    Path tmpPath = new Path("/tmp/file");
    doReturn(new FileStatus(1024, false, 1, 4096, 37, 42,  FsPermission.createImmutable((short) 0644), "root", "wheel", tmpPath)).when(mockLocalFS).getFileStatus(tmpPath);

    Path path = new Path("/tmp/10.0.0.1@file");
    FileStatus status = fs.getFileStatus(path);

    assertEquals(new Path("pdfs:/tmp/10.0.0.1@file"), status.getPath());
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
  public void testGetFileStatusWithLocalPath() throws IOException {
    try {
      Path path = new Path("/tmp/file");
      @SuppressWarnings("unused")
      FileStatus status = fs.getFileStatus(path);
      fail("Expected getFileStatus call to throw an exception");
    } catch (IOException e) {
      // ok
    }
  }

  @Test
  public void testGetFileStatusWithUnknownRemotePath() throws IOException {
    Path path = new Path("/tmp/10.0.0.3@file");
    try {
      fs.getFileStatus(path);
      fail("Expected getFileStatus to throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // ok
    }
  }

  @Test
  public void testListStatusRoot() throws IOException {
    Path root = new Path("/");
    FileStatus[] statuses = fs.listStatus(root);

    assertEquals(1, statuses.length);

    assertEquals(new Path("pdfs:/foo"), statuses[0].getPath());
    assertTrue(statuses[0].isDirectory());
    assertEquals(0755, statuses[0].getPermission().toExtendedShort());
  }

  @Test
  public void testListStatusWithValidPath() throws IOException {
    Path path = new Path("/foo/bar");
    FileStatus[] statuses = fs.listStatus(path);

    assertEquals(5, statuses.length);

    // Note -- listStatus does not guarantee ordering. Sorting/then searching for paths
    // rather than relying on fixed indices.
    Arrays.sort(statuses);

    final FileStatus searchKeyStatus = new FileStatus();
    searchKeyStatus.setPath(new Path("pdfs:/foo/bar/10.0.0.1@file1"));
    int testIndex = Arrays.binarySearch(statuses, searchKeyStatus);
    assertTrue("Status for path " + searchKeyStatus.getPath().toString() + " not found", testIndex >= 0);
    assertEquals(new Path("pdfs:/foo/bar/10.0.0.1@file1"), statuses[testIndex].getPath());
    assertFalse(statuses[testIndex].isDirectory());
    assertEquals(1024, statuses[testIndex].getLen());
    assertEquals(1, statuses[testIndex].getReplication());
    assertEquals(4096, statuses[testIndex].getBlockSize());
    assertEquals(42, statuses[testIndex].getAccessTime());
    assertEquals(37, statuses[testIndex].getModificationTime());
    assertEquals("root", statuses[testIndex].getOwner());
    assertEquals("wheel", statuses[testIndex].getGroup());
    assertEquals(0644, statuses[testIndex].getPermission().toExtendedShort());


    searchKeyStatus.setPath(new Path("pdfs:/foo/bar/10.0.0.1@file2"));
    testIndex = Arrays.binarySearch(statuses, searchKeyStatus);
    assertTrue("Status for path " + searchKeyStatus.getPath().toString() + " not found", testIndex >= 0);
    assertEquals(new Path("pdfs:/foo/bar/10.0.0.1@file2"), statuses[testIndex].getPath());
    assertFalse(statuses[1].isDirectory());
    assertEquals(2048, statuses[testIndex].getLen());
    assertEquals(1, statuses[testIndex].getReplication());
    assertEquals(4096, statuses[testIndex].getBlockSize());
    assertEquals(42, statuses[testIndex].getAccessTime());
    assertEquals(37, statuses[testIndex].getModificationTime());
    assertEquals("root", statuses[testIndex].getOwner());
    assertEquals("wheel", statuses[testIndex].getGroup());
    assertEquals(0644, statuses[testIndex].getPermission().toExtendedShort());


    searchKeyStatus.setPath(new Path("pdfs:/foo/bar/10.0.0.2@file1"));
    testIndex = Arrays.binarySearch(statuses, searchKeyStatus);
    assertTrue("Status for path " + searchKeyStatus.getPath().toString() + " not found", testIndex >= 0);
    assertEquals(new Path("pdfs:/foo/bar/10.0.0.2@file1"), statuses[testIndex].getPath());
    assertFalse(statuses[testIndex].isDirectory());
    assertEquals(1027, statuses[testIndex].getLen());
    assertEquals(1, statuses[testIndex].getReplication());
    assertEquals(4096, statuses[testIndex].getBlockSize());
    assertEquals(42, statuses[testIndex].getAccessTime());
    assertEquals(37, statuses[testIndex].getModificationTime());
    assertEquals("root", statuses[testIndex].getOwner());
    assertEquals("wheel", statuses[testIndex].getGroup());
    assertEquals(0644, statuses[testIndex].getPermission().toExtendedShort());


    searchKeyStatus.setPath(new Path("pdfs:/foo/bar/10.0.0.2@file3"));
    testIndex = Arrays.binarySearch(statuses, searchKeyStatus);
    assertTrue("Status for path " + searchKeyStatus.getPath().toString() + " not found", testIndex >= 0);
    assertEquals(new Path("pdfs:/foo/bar/10.0.0.2@file3"), statuses[testIndex].getPath());
    assertFalse(statuses[testIndex].isDirectory());
    assertEquals(2049, statuses[testIndex].getLen());
    assertEquals(1, statuses[testIndex].getReplication());
    assertEquals(4096, statuses[testIndex].getBlockSize());
    assertEquals(42, statuses[testIndex].getAccessTime());
    assertEquals(37, statuses[testIndex].getModificationTime());
    assertEquals("root", statuses[testIndex].getOwner());
    assertEquals("wheel", statuses[testIndex].getGroup());
    assertEquals(0644, statuses[testIndex].getPermission().toExtendedShort());


    searchKeyStatus.setPath(new Path("pdfs:/foo/bar/dir"));
    testIndex = Arrays.binarySearch(statuses, searchKeyStatus);
    assertTrue("Status for path " + searchKeyStatus.getPath().toString() + " not found", testIndex >= 0);
    assertEquals(new Path("pdfs:/foo/bar/dir"), statuses[testIndex].getPath());
    assertTrue(statuses[testIndex].isDirectory());
    assertEquals(47, statuses[testIndex].getLen());
    assertEquals(0, statuses[testIndex].getReplication());
    assertEquals(0, statuses[testIndex].getBlockSize());
    assertEquals(3645, statuses[testIndex].getAccessTime());
    assertEquals(1234, statuses[testIndex].getModificationTime());
    assertEquals("admin", statuses[testIndex].getOwner());
    assertEquals("admin", statuses[testIndex].getGroup());
    assertEquals(0755, statuses[testIndex].getPermission().toExtendedShort());
  }

  @Test
  public void testListStatusWithUnknownRemotePath() throws IOException {
    Path path = new Path("/foo/10.0.0.3@bar");
    try {
      fs.listStatus(path);
      fail("Expected getFileStatus to throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // ok
    }
  }

  @Test
  public void testListStatusIteratorPastLastElement() throws IOException {
    final Path root = new Path("/");
    final RemoteIterator<FileStatus> statusIter = fs.listStatusIterator(root);

    while (statusIter.hasNext()) {
      statusIter.next();
    }

    try {
      statusIter.next();
      fail("NoSuchElementException should be throw when next() is called when there are no elements remaining.");
    } catch (NoSuchElementException ex) {
      // OK.
    }
  }

  @Test
  public void testListStatusIteratorRoot() throws IOException {
    final Path root = new Path("/");
    final RemoteIterator<FileStatus> statusIterator = fs.listStatusIterator(root);

    assertTrue(statusIterator.hasNext());

    final FileStatus onlyStatus = statusIterator.next();
    assertEquals(new Path("pdfs:/foo"), onlyStatus.getPath());
    assertTrue(onlyStatus.isDirectory());
    assertEquals(0755, onlyStatus.getPermission().toExtendedShort());

    assertTrue(!statusIterator.hasNext());
  }

  @Test
  public void testOpenRoot() throws IOException {
    Path root = new Path("/");
    try {
      fs.open(root);
      fail("Expected open call to throw an exception");
    } catch (AccessControlException e) {
      // ok
    }
  }

  @Test
  public void testOpenFile() throws IOException {
    FSDataInputStream mockFDIS = mock(FSDataInputStream.class);
    doReturn(mockFDIS).when(mockLocalFS).open(new Path("/foo/bar/file"), 32768);

    Path root = new Path("/foo/bar/10.0.0.1@file");
    FSDataInputStream fdis = fs.open(root, 32768);
    assertNotNull(fdis);
  }

  @Test
  public void testOpenLocalFile() throws IOException {
    try {
      Path path = new Path("/tmp/file");
      @SuppressWarnings("unused")
      FSDataInputStream fdis = fs.open(path, 32768);
      fail("Expected open call to throw an exception");
    } catch (IOException e) {
      // ok
    }
  }

  @Test
  public void testCreateRoot() throws IOException {
    Path root = new Path("/");
    try {
      fs.create(root, FsPermission.getFileDefault(), true, 0, (short) 1, 4096, null);
      fail("Expected create call to throw an exception");
    } catch (AccessControlException e) {
      // ok
    }
  }

  @Test
  public void testCreateFile() throws IOException {
    FSDataOutputStream mockFDOS = mock(FSDataOutputStream.class);
    doReturn(mockFDOS).when(mockLocalFS).create(
            new Path("/foo/bar/file"),
            FsPermission.getFileDefault(),
            true,
            0,
            (short) 1,
            4096L,null);

    Path path = new Path("/foo/bar/10.0.0.1@file");
    FSDataOutputStream fdos = fs.create(path, FsPermission.getFileDefault(), true, 0, (short) 1, 4096, null);
    assertNotNull(fdos);
  }

  @Test
  public void testCreateLocalFile() throws IOException {
    try {
      Path path = new Path("foo/bar/file");
      @SuppressWarnings("unused")
      FSDataOutputStream fdos = fs.create(path, FsPermission.getFileDefault(), true, 0, (short) 1, 4096, null);
      fail("Expected create call to throw an exception");
    } catch (IOException e) {
      // ok
    }
  }

  @Test
  public void testAppendRoot() throws IOException {
    Path root = new Path("/");
    try {
      fs.append(root, 4096, null);
      fail("Expected append call to throw an exception");
    } catch (AccessControlException e) {
      // ok
    }
  }

  @Test
  public void testAppendFile() throws IOException {
    FSDataOutputStream mockFDOS = mock(FSDataOutputStream.class);
    doReturn(mockFDOS).when(mockRemoteFS).append(
            new Path("/foo/bar/file"),
            4096,
            null);

    Path path = new Path("/foo/bar/10.0.0.2@file");
    FSDataOutputStream fdos = fs.append(path, 4096, null);
    assertNotNull(fdos);
  }

  @Test
  public void testAppendLocalFile() throws IOException {
    try {
      Path path = new Path("/foo/bar/file");
      @SuppressWarnings("unused")
      FSDataOutputStream fdos = fs.append(path, 4096, null);
      fail("Expected append call to throw an exception");
    } catch (IOException e) {
      // ok
    }
  }

  @Test
  public void testDeleteRoot() throws IOException {
    Path root = new Path("/");
    try {
      fs.delete(root, false);
      fail("Expected delete call to throw an exception");
    } catch (AccessControlException e) {
      // ok
    }
  }

  @Test
  public void testDeleteLocalFile() throws IOException {
    try {
      Path path = new Path("/foo/bar/file");
      fs.delete(path, false);
      fail("Expected delete call to throw an exception");
    } catch (IOException e) {
      // ok
    }
  }

  @Test
  public void testDeleteFile() throws IOException {
    doReturn(true).when(mockRemoteFS).delete(
        new Path("/foo/bar"),
        false);

    Path path = new Path("/foo/10.0.0.2@bar");
    assertTrue(fs.delete(path, false));
  }

  @Test
  public void testDeleteUnknownLocalFile() throws IOException {
    doThrow(FileNotFoundException.class).when(mockLocalFS).delete(
        new Path("/foo/unknown"),
        false);

    Path path = new Path("/foo/10.0.0.1@unknown");
    try{
      fs.delete(path, false);
      fail("Expecting FileNotFoundException");
    } catch(FileNotFoundException e) {
      // nothing
    }
  }

  @Test
  public void testDeleteUnknownRemoteFile() throws IOException {
    doThrow(FileNotFoundException.class).when(mockRemoteFS).delete(
        new Path("/foo/unknown"),
        false);

    Path path = new Path("/foo/10.0.0.2@unknown");
    try{
      fs.delete(path, false);
      fail("Expecting FileNotFoundException");
    } catch(FileNotFoundException e) {
      // nothing
    }
  }

  @Test
  public void testMkdirsRoot() throws IOException {
    Path root = new Path("/");

    assertTrue(fs.mkdirs(root, FsPermission.getDirDefault()));
  }

  @Test
  public void testMkdirsRemoteFile() throws IOException {
    doReturn(true).when(mockLocalFS).mkdirs(
        new Path("/foo/bar/dir2"),
        FsPermission.getFileDefault());
    doReturn(true).when(mockRemoteFS).mkdirs(
        new Path("/foo/bar/dir2"),
        FsPermission.getFileDefault());

    Path path = new Path("/foo/bar/dir2");
    assertTrue(fs.mkdirs(path, FsPermission.getFileDefault()));
  }

  @Test
  public void testRenameFromRoot() throws IOException {
    Path root = new Path("/");
    Path dst = new Path("/foo/baz");

    try {
      fs.rename(root, dst);
      fail("Expected rename to throw an exception");
    } catch(IOException e) {
      // ok
    }
  }

  @Test
  public void testRenameToRoot() throws IOException {
    Path root = new Path("/");
    Path src = new Path("/foo/bar");

    try {
      fs.rename(src, root);
      fail("Expected rename to throw an exception");
    } catch(IOException e) {
      // ok
    }
  }

  @Test
  public void testRenameFileSameEndpoint() throws IOException {
    doReturn(true).when(mockLocalFS).rename(
        new Path("/foo/bar/file1"),
        new Path("/foo/bar/file3"));

    Path src = new Path("/foo/bar/10.0.0.1@file1");
    Path dst = new Path("/foo/bar/10.0.0.1@file3");
    assertTrue(fs.rename(src, dst));
  }

  @Test
  public void testRenameRemoteFileDifferentEndpoints() throws IOException {
    Path src = new Path("/foo/bar/10.0.0.1@file1");
    Path dst = new Path("/foo/bar/10.0.0.2@file3");
    try {
      fs.rename(src, dst);
      fail("Expected rename across endpoints to fail");
    } catch(IOException e) {
      // ok
    }
  }

  @Test
  public void testRenameDirectories() throws IOException {
    doReturn(true).when(mockLocalFS).rename(
        new Path("/foo/bar"),
        new Path("/foo/baz"));
    doReturn(true).when(mockRemoteFS).rename(
        new Path("/foo/bar"),
        new Path("/foo/baz"));

    Path src = new Path("/foo/bar");
    Path dst = new Path("/foo/baz");
    assertTrue(fs.rename(src, dst));
  }

  @Test
  public void testGetFileBlockLocations() throws IOException {
    Path path = new Path("/foo/10.0.0.1@bar");

    BlockLocation[] locations = fs.getFileBlockLocations(new FileStatus(1027, false, 1, 4096, 123456, path), 7, 1024);
    assertEquals(1, locations.length);

    // BlockLocation has no good equals method
    BlockLocation location = locations[0];
    assertArrayEquals(new String[] { "10.0.0.1:1234", "10.0.0.1:9012" }, location.getNames());
    assertArrayEquals(new String[] { "10.0.0.1" }, location.getHosts());
    // Block covers the whole file
    assertEquals(0, location.getOffset());
    assertEquals(1027, location.getLength());
  }

  @Test
  public void testGetFileBlockLocationsStartGreaterThanLen() throws IOException {
    Path path = new Path("/foo/10.0.0.1@bar");

    assertArrayEquals(new BlockLocation[]{}, fs.getFileBlockLocations(new FileStatus(1024, false, 1,4096, 123456, path), 4096, 8192));
  }

  @Test
  public void testGetFileBlockLocationsNegativeStart() throws IOException {
    Path path = new Path("/foo/10.0.0.1@bar");

    try {
      fs.getFileBlockLocations(new FileStatus(1024, false, 1,4096, 123456, path), -22, 8192);
      fail("Expected getFileBlockLocation to throw an exception");
    } catch (IllegalArgumentException e) {
      // ok
    }
  }

  @Test
  public void testGetFileBlockLocationsNegativeLen() throws IOException {
    Path path = new Path("/foo/10.0.0.1@bar");

    try {
      fs.getFileBlockLocations(new FileStatus(1024, false, 1,4096, 123456, path), 18, -2);
      fail("Expected getFileBlockLocation to throw an exception");
    } catch (IllegalArgumentException e) {
      // ok
    }
  }

  @Test
  public void testGlobStatusEmptyDir() throws IOException {
    Path path = new Path("/foo/bar/dir");

    FileStatus[] statuses = fs.globStatus(path);

    assertEquals(1, statuses.length);

  }

  @Test
  public void testCanonicalizeRemoteFile() throws IOException {
    Path path = new Path("/foo/bar/10.0.0.2@file");

    Path resolvedPath = fs.canonicalizePath(path);
    assertEquals(new Path("/foo/bar/10.0.0.2@file"), resolvedPath);
  }

  @Test
  public void testCanonicalizeDirectoryFile() throws IOException {
    Path path = new Path("/foo/bar");

    Path resolvedPath = fs.canonicalizePath(path);
    assertEquals(new Path("/foo/bar"), resolvedPath);
  }

  @Test
  public void testCanonicalizeLocalFile() throws IOException {
    Path path = new Path("/foo/bar/file");

    Path resolvedPath = fs.canonicalizePath(path);
    assertEquals(new Path("/foo/bar/10.0.0.1@file"), resolvedPath);
  }

  @Test
  public void testCanonicalizeLocalFileIfNoLocalAccess() throws IOException {
    Provider<Iterable<NodeEndpoint>> endpointsProvider =
        DirectProvider.<Iterable<NodeEndpoint>>wrap((Arrays.asList(REMOTE_ENDPOINT_1, REMOTE_ENDPOINT_2)));
    PDFSConfig pdfsConfig = new PDFSConfig(MoreExecutors.newDirectExecutorService(), null, null, endpointsProvider, LOCAL_ENDPOINT, false);
    PseudoDistributedFileSystem pdfs = newPseudoDistributedFileSystem(pdfsConfig);
    Path path = new Path("/foo/bar/file");

    Path resolvedPath = pdfs.canonicalizePath(path);
    assertEquals(new Path("/foo/bar/10.0.0.2@file"), resolvedPath);
  }
}
