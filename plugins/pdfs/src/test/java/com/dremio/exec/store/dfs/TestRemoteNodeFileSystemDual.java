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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.CoordinationProtos.Roles;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.store.dfs.PDFSService.PDFSMode;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.BaseTestFabric;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * End-to-end test for {@link RemoteNodeFileSystem} using a Fabric server
 */
public class TestRemoteNodeFileSystemDual extends BaseTestFabric {

  @ClassRule public static final  TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final ExpectedException exception = ExpectedException.none();

  private static BufferAllocator root;
  private static ServiceHolder data;
  private static ServiceHolder client;
  private static FileSystem clientFS;

  @BeforeClass
  public static void setUpPDFSService() throws Exception {
    root = new RootAllocator(Long.MAX_VALUE);
    EndpointProvider provider = new EndpointProvider();
    client = new ServiceHolder(root, provider, PDFSMode.CLIENT, "client");
    data = new ServiceHolder(root, provider, PDFSMode.DATA, "data");
    provider.add(data);
    clientFS = client.fileSystem;
  }

  @AfterClass
  public static void teardown() throws Exception{
    AutoCloseables.close(data, client, root);
  }

  @Test
  public void basicClientReadWrite() throws Exception {
    Path basePath = new Path(temporaryFolder.newFolder().getAbsolutePath());
    Path path = ((PathCanonicalizer) clientFS).canonicalizePath(new Path(basePath, "testfile.bytes"));
    final byte[] randomBytesMoreThanBuffer = new byte[RemoteNodeFileSystem.REMOTE_WRITE_BUFFER_SIZE * 3];
    Random r = new Random();
    r.nextBytes(randomBytesMoreThanBuffer);

    try(FSDataOutputStream stream = clientFS.create(path, false)){
      stream.write(randomBytesMoreThanBuffer);
    }


    RemoteIterator<LocatedFileStatus> iter = client.fileSystem.listFiles(basePath, false);
    assertEquals(true, iter.hasNext());
    LocatedFileStatus status = iter.next();

    try(FSDataInputStream in = clientFS.open(status.getPath())){
      byte[] back = new byte[randomBytesMoreThanBuffer.length];
      int dataRead = in.read(back);
      assertEquals(back.length, dataRead);
      assertTrue(Arrays.equals(randomBytesMoreThanBuffer, back));
    }
    client.fileSystem.delete(status.getPath(), false);
  }

  private static class ServiceHolder implements AutoCloseable {

    private final CloseableThreadPool pool;
    private final FabricServiceImpl fabric;
    private final BufferAllocator allocator;
    private final NodeEndpoint endpoint;
    private final PDFSService service;
    private final FileSystem fileSystem;

    public ServiceHolder(BufferAllocator allocator, Provider<Iterable<NodeEndpoint>> nodeProvider, PDFSMode mode, String name) throws Exception{
      this.allocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
      pool = new CloseableThreadPool(name);
      fabric = new FabricServiceImpl("localhost", 9970, true, 0, 2, pool, this.allocator, 0, Long.MAX_VALUE);
      fabric.start();

      endpoint = NodeEndpoint.newBuilder()
          .setAddress(fabric.getAddress()).setFabricPort(fabric.getPort())
          .setRoles(Roles.newBuilder().setJavaExecutor(mode == PDFSMode.DATA))
          .build();

      service = new PDFSService(DirectProvider.wrap((FabricService) fabric), DirectProvider.wrap(endpoint), nodeProvider, DremioTest.DEFAULT_SABOT_CONFIG, this.allocator, mode);
      service.start();
      fileSystem = service.createFileSystem();
    }

    public NodeEndpoint getEndpoint(){
      return endpoint;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(pool, service, fabric, allocator);
    }
  }

  private static class EndpointProvider implements Provider<Iterable<NodeEndpoint>> {
    private List<ServiceHolder> holders = new ArrayList<>();

    @Override
    public Collection<NodeEndpoint> get() {
      return FluentIterable.from(holders).transform(new Function<ServiceHolder, NodeEndpoint>() {
        @Override
        public NodeEndpoint apply(ServiceHolder input) {
          return input.getEndpoint();
        }
      }).toList();
    }

    public void add(ServiceHolder holder){
      holders.add(holder);
    }
  }
}
