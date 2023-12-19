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
package com.dremio.service.conduit;

import static com.dremio.service.conduit.ConduitTestServiceGrpc.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.service.conduit.ConduitTestServiceGrpc.ConduitTestServiceBlockingStub;
import com.dremio.service.grpc.GrpcProxy;
import com.dremio.service.grpc.GrpcProxyHandlerRegistry;
import com.google.common.base.Predicates;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

public class TestGrpcProxy {

  private static final int DELAY_SECS = 5; // Change this for testing different delays
  private static final Random RANDOM = new Random();

  /**
   * Manages automatic graceful shutdown for the registered servers and channels.
   */
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Test
  public void testProxy() throws IOException {
    ManagedChannel targetChannel = startTargetService();
    ManagedChannel proxyChannel = startProxyService(targetChannel);
    final ConduitTestServiceBlockingStub stub =
        ConduitTestServiceGrpc.newBlockingStub(proxyChannel);
    Assert.assertEquals(1, stub.increment(IncrementRequest.newBuilder().setI(0).build()).getI());
  }

  @Test
  public void testDelayAtTargetService() throws IOException {
    ManagedChannel targetChannel = startTargetService();
    ManagedChannel proxyChannel = startProxyService(targetChannel);
    final ConduitTestServiceBlockingStub stub =
        ConduitTestServiceGrpc.newBlockingStub(proxyChannel);
    Assert.assertEquals(DELAY_SECS + 1, stub.increment(IncrementRequest.newBuilder()
        .setI(DELAY_SECS).build()).getI());
  }

  @Test
  public void testDelayAtClient() throws IOException {
    ManagedChannel targetChannel = startTargetService();
    ManagedChannel proxyChannel = startProxyService(targetChannel);
    final ConduitTestServiceBlockingStub stub =
        ConduitTestServiceGrpc.newBlockingStub(proxyChannel);
    Assert.assertEquals(1, stub.increment(IncrementRequest.newBuilder()
        .setI(0).build()).getI());

    sleepUninterruptibly(DELAY_SECS, TimeUnit.SECONDS);
    Assert.assertEquals(1, stub.increment(IncrementRequest.newBuilder()
        .setI(0).build()).getI());
  }

  @Test
  public void testDelayAtStreamingClient() throws IOException {
    ManagedChannel targetChannel = startTargetService();
    ManagedChannel proxyChannel = startProxyService(targetChannel);
    final ConduitTestServiceBlockingStub stub =
      ConduitTestServiceGrpc.newBlockingStub(proxyChannel);
    MutableInt expected = new MutableInt(1);
    stub.nIncrements(IncrementRequest.newBuilder().setI(0).build()).forEachRemaining(res -> {
      optionalDelay();
      Assert.assertEquals(expected.getAndIncrement(), res.getI());
    });
  }

  @Test
  public void testInapplicableService() throws IOException {
    ManagedChannel targetChannel = startTargetService();
    ManagedChannel proxyChannel = startProxyService(targetChannel, Predicates.alwaysFalse());
    final ConduitTestServiceBlockingStub stub =
        ConduitTestServiceGrpc.newBlockingStub(proxyChannel);

    assertThatThrownBy(() -> stub.increment(IncrementRequest.newBuilder()
        .setI(0).build()).getI())
        .hasMessage("UNIMPLEMENTED: Method not found: %s/Increment", SERVICE_NAME);
  }

  private static void optionalDelay() {
    if (RANDOM.nextInt(5) == 0) {// delay one of 5 requests
      sleepUninterruptibly(DELAY_SECS, TimeUnit.SECONDS);
    }
  }

  private ManagedChannel startProxyService(ManagedChannel targetChannel)
      throws IOException {
    return startProxyService(targetChannel, Predicates.alwaysTrue());
  }

  private ManagedChannel startProxyService(ManagedChannel targetChannel, Predicate<String> criteria)
      throws IOException {
    String serverName = InProcessServerBuilder.generateName();

    ManagedChannel nessieServiceChannel = InProcessChannelBuilder.forName("proxy").build();
    this.grpcCleanup.register(nessieServiceChannel);

    Server proxyServer = InProcessServerBuilder.forName(serverName).directExecutor()
        .fallbackHandlerRegistry(new GrpcProxyHandlerRegistry(new GrpcProxy<>(() -> targetChannel),
            criteria))
        .build();

    this.grpcCleanup.register(proxyServer.start());
    ManagedChannel proxyChannel = InProcessChannelBuilder.forName(serverName)
        .build();
    this.grpcCleanup.register(proxyChannel);
    return proxyChannel;
  }

  private ManagedChannel startTargetService() throws IOException {
    String serverName = InProcessServerBuilder.generateName();

    ManagedChannel nessieServiceChannel = InProcessChannelBuilder.forName("conduit")
        .build();
    this.grpcCleanup.register(nessieServiceChannel);
    Server testServer = InProcessServerBuilder.forName(serverName).directExecutor()
        .addService(new TestService()).build();

    this.grpcCleanup.register(testServer.start());
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
    this.grpcCleanup.register(channel);
    return channel;
  }

  private static final class TestService extends ConduitTestServiceGrpc.ConduitTestServiceImplBase {
    @Override
    public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
      sleepUninterruptibly(request.getI(), TimeUnit.SECONDS);
      responseObserver.onNext(IncrementResponse.newBuilder().setI(request.getI() + 1).build());
      responseObserver.onCompleted();
    }

    @Override
    public void nIncrements(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
      MutableInt res = new MutableInt(request.getI());
      IntStream.range(0, 10).forEach(i -> {
        optionalDelay();
        responseObserver.onNext(IncrementResponse.newBuilder().setI(res.incrementAndGet()).build());
      });
      optionalDelay();
      responseObserver.onCompleted();
    }
  }

  private static void sleepUninterruptibly(long sleepFor, TimeUnit unit) {
    boolean interrupted = false;
    try {
      long remainingNanos = unit.toNanos(sleepFor);
      long end = System.nanoTime() + remainingNanos;
      while (true) {
        try {
          NANOSECONDS.sleep(remainingNanos);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
          remainingNanos = end - System.nanoTime();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
