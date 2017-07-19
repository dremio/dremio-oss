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

import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.services.fabric.BaseTestFabric;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.test.DremioTest;

/**
 * End-to-end test for {@link RemoteNodeFileSystem} using a Fabric server
 */
public class TestRemoteNodeFileSystemE2E extends BaseTestFabric {

  @ClassRule public static final  TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final ExpectedException exception = ExpectedException.none();

  private RemoteNodeFileSystem sabotFS;

  @Before
  public void setUpPDFSService() throws IOException {
    NodeEndpoint endpoint = NodeEndpoint.newBuilder()
        .setAddress(fabric.getAddress()).setFabricPort(fabric.getPort())
        .build();
    PDFSProtocol pdfsProtocol = PDFSProtocol.newInstance(endpoint, DremioTest.DEFAULT_SABOT_CONFIG, allocator, true);
    FabricRunnerFactory factory = fabric.registerProtocol(pdfsProtocol);
    sabotFS = new RemoteNodeFileSystem(factory.getCommandRunner(fabric.getAddress(), fabric.getPort()), allocator);
    sabotFS.initialize(URI.create(format("sabot://%s:%d", fabric.getAddress(), fabric.getPort())), new Configuration(false));
  }

  @Test
  public void testGetFileStatus() throws IOException {
    File subFolder = temporaryFolder.newFolder();

    java.nio.file.Path nativePath = Files.createFile(
        new File(subFolder, "foo").toPath(),
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("r-xr--r--")));

    FileStatus status = sabotFS.getFileStatus(new Path(toPathString(nativePath)));
    assertEquals(toPathString(nativePath), toPathString(status.getPath()));
    assertEquals(FsPermission.createImmutable((short) 0544), status.getPermission());
  }

  @Test
  public void testListStatus() throws IOException {
    File subFolder = temporaryFolder.newFolder();

    java.nio.file.Path nativeFooPath = Files.createFile(new File(subFolder, "foo").toPath());
    java.nio.file.Path nativeBarPath = Files.createFile(new File(subFolder, "bar").toPath());
    java.nio.file.Path nativeBazPath = Files.createDirectory(new File(subFolder, "baz").toPath());

    FileStatus[] statuses = sabotFS.listStatus(new Path(subFolder.getAbsolutePath()));
    Map<String, FileStatus> statusMap = new HashMap<>();
    for(FileStatus status: statuses) {
      statusMap.put(toPathString(status.getPath()), status);
    }

    assertEquals(3, statusMap.size());
    {
      FileStatus status = statusMap.get(toPathString(nativeFooPath));
      assertFalse(status.isDirectory());
    }
    {
      FileStatus status = statusMap.get(toPathString(nativeBarPath));
      assertFalse(status.isDirectory());
    }
    {
      FileStatus status = statusMap.get(toPathString(nativeBazPath));
      assertTrue(status.isDirectory());
    }
  }

  @Test
  public void testGetStatusThrowIOException() throws IOException {
    exception.expect(FileNotFoundException.class);
    sabotFS.getFileStatus(new Path("/foo/bar/shouldnotexist"));
  }

  @Test
  public void testMkdirs() throws IOException {
    File subFolder = temporaryFolder.newFolder();

    boolean res = sabotFS.mkdirs(new Path(subFolder.getAbsolutePath(), "foo"));

    assertTrue(res);
    assertTrue(Files.exists(new File(subFolder, "foo").toPath()));
  }

  @Test
  public void testRename() throws IOException {
    File subFolder = temporaryFolder.newFolder();
    java.nio.file.Path nativeFooPath = Files.createFile(new File(subFolder, "foo").toPath());

    boolean res = sabotFS.rename(new Path(toPathString(nativeFooPath)), new Path(subFolder.getAbsolutePath(), "foo2"));

    assertTrue(res);
    assertTrue(Files.exists(new File(subFolder, "foo2").toPath()));
  }

  @Test
  public void testDelete() throws IOException {
    File subFolder = temporaryFolder.newFolder();
    java.nio.file.Path nativeFooPath = Files.createFile(new File(subFolder, "foo").toPath());

    boolean res = sabotFS.delete(new Path(toPathString(nativeFooPath)), false);

    assertTrue(res);
    assertFalse(Files.exists(new File(subFolder, "foo").toPath()));
  }

  @Test
  public void testOpen() throws IOException {
    byte[] data = new byte[8192];
    for(int i = 0; i<data.length; i++) {
      data[i] = (byte) (i % 256);
    }

    File subFolder = temporaryFolder.newFolder();
    java.nio.file.Path nativeFooPath = Files.createFile(new File(subFolder, "foo").toPath());
    Files.write(nativeFooPath, data);


    byte[] readData = new byte[8192];
    int offset = 0;

    try(FSDataInputStream fdis = sabotFS.open(new Path(toPathString(nativeFooPath)), 1024)) {
      while(true) {
        int res = fdis.read(readData, offset, readData.length - offset);
        if (res == -1) {
          break;
        }
        offset += res;
        assertTrue(offset <= readData.length);
      }
    }

    assertEquals(readData.length, offset);
    assertArrayEquals(data, readData);

  }

  private static String toPathString(Path path) {
    return path.toUri().getPath().replaceAll("/$", "");
  }

  private static String toPathString(java.nio.file.Path path) {
    return path.toUri().getPath().replaceAll("/$", "");
  }

}
