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
package com.dremio.services.fabric;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.rpc.RpcException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;

import io.netty.buffer.ArrowBuf;

/**
 * A basic framework for registering and testing new rpc protocols.
 */
public class BaseTestFabric {

  protected static final int THREAD_COUNT = 2;
  protected static final long RESERVATION = 0;
  protected static final long MAX_ALLOCATION = Long.MAX_VALUE;
  protected static final int TIMEOUT = 300;

  @VisibleForTesting protected BufferAllocator allocator;
  @VisibleForTesting protected CloseableThreadPool pool;
  @VisibleForTesting protected FabricServiceImpl fabric;
  @VisibleForTesting protected Random random = new Random();
  @VisibleForTesting protected final String address = "localhost";

  // some default values for a simple protocol.
  @VisibleForTesting protected final QueryId expectedQ = QueryId.newBuilder()
      .setPart1(random.nextLong())
      .setPart2(random.nextLong())
      .build();

  @VisibleForTesting protected final NodeEndpoint expectedD = NodeEndpoint.newBuilder()
      .setAddress("a random value")
      .setFabricPort(random.nextInt())
      .setUserPort(random.nextInt())
      .build();

  @Before
  public void setupServer() throws Exception{
    allocator = new RootAllocator(20 * 1024 * 1024);
    pool = new CloseableThreadPool("test-fabric");
    fabric = new FabricServiceImpl(address, 45678, true, THREAD_COUNT, allocator, RESERVATION,
        MAX_ALLOCATION, TIMEOUT, pool);
    fabric.start();
  }

  @After
  public void shutdown() throws Exception{
    AutoCloseables.close(fabric, pool, allocator);
  }

  public static void assertEqualsRpc(byte[] expected, ArrowBuf actual) throws RpcException{
    try{
      assertEqualsBytes(expected, actual);
    }catch(AssertionError e){
      throw new RpcException(e);
    }
  }

  public static void assertEqualsBytes(byte[] expected, ArrowBuf actual) {
    if(expected == null && actual == null){
      return;
    }

    Preconditions.checkNotNull(actual);
    Preconditions.checkNotNull(expected);
    byte[] incoming = new byte[1024];
    actual.getBytes(0, incoming);
    assertTrue("Data was not equal.", Arrays.equals(incoming, expected));
  }

  /**
   * A class that allows EnumLites to be directly defined.
   */
  public static class FakeEnum implements EnumLite {
    private final int number;

    public FakeEnum(int number) {
      super();
      this.number = number;
    }

    @Override
    public int getNumber() {
      return number;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + number;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      FakeEnum other = (FakeEnum) obj;
      if (number != other.number) {
        return false;
      }
      return true;
    }

  }
}

