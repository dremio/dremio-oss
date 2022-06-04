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
package com.dremio.services.nessie.grpc.client;

import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc.ConfigServiceImplBase;
import com.dremio.services.nessie.grpc.api.Content;
import com.dremio.services.nessie.grpc.api.ContentRequest;
import com.dremio.services.nessie.grpc.api.ContentServiceGrpc.ContentServiceImplBase;
import com.dremio.services.nessie.grpc.api.ContentWithKey;
import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.GetAllReferencesRequest;
import com.dremio.services.nessie.grpc.api.GetAllReferencesResponse;
import com.dremio.services.nessie.grpc.api.IcebergTable;
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;
import com.dremio.services.nessie.grpc.api.NessieConfiguration;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceImplBase;
import com.google.common.collect.ImmutableList;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

/**
 * Tests constructs for Nessie client tests
 */
public abstract class AbstractTestNessieGrpcClient {

  public static final Content ICEBERG_TABLE =
      Content.newBuilder().setIceberg(IcebergTable.newBuilder().build()).build();
  public static final String REF_NAME = "test-main";
  public static final Reference REF = Branch.of(REF_NAME, null);
  public static final Metadata.Key<String> TEST_HEADER_KEY = Metadata.Key.of("test_header", Metadata.ASCII_STRING_MARSHALLER);

  /**
   * Manages automatic graceful shutdown for the registered servers and channels.
   */
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * Just a dummy implementation of a gRPC service to simulate a client call.
   */
  private final TreeServiceImplBase treeService =
      new TreeServiceImplBase() {
        @Override
        public void getAllReferences(
            GetAllReferencesRequest request, StreamObserver<GetAllReferencesResponse> responseObserver) {
          responseObserver.onNext(
              GetAllReferencesResponse.newBuilder().addReference(refToProto(REF)).build());
          responseObserver.onCompleted();
        }

        @Override
        public void getDefaultBranch(
            Empty request, StreamObserver<com.dremio.services.nessie.grpc.api.Reference> responseObserver) {
          responseObserver.onNext(
              com.dremio.services.nessie.grpc.api.Reference.newBuilder()
                  .setBranch(
                      com.dremio.services.nessie.grpc.api.Branch.newBuilder().setName(REF_NAME).build())
                  .build());
          responseObserver.onCompleted();
        }
      };

  /**
   * Just a dummy implementation of a gRPC service to simulate a client call.
   */
  private final ContentServiceImplBase contentService =
      new ContentServiceImplBase() {
        @Override
        public void getContent(
            ContentRequest request, StreamObserver<Content> responseObserver) {
          responseObserver.onNext(ICEBERG_TABLE);
          responseObserver.onCompleted();
        }

        @Override
        public void getMultipleContents(
            MultipleContentsRequest request,
            StreamObserver<MultipleContentsResponse> responseObserver) {
          responseObserver.onNext(
              MultipleContentsResponse.newBuilder()
                  .addContentWithKey(
                      ContentWithKey.newBuilder().setContentKey(
                              com.dremio.services.nessie.grpc.api.ContentKey.newBuilder().addElements("foo"))
                          .setContent(ICEBERG_TABLE).build())
                  .build());
          responseObserver.onCompleted();
        }
      };

  /**
   * Just a dummy implementation of a gRPC service to simulate a client call.
   */
  private final ConfigServiceImplBase configService =
      new ConfigServiceImplBase() {
        @Override
        public void getConfig(Empty request, StreamObserver<NessieConfiguration> responseObserver) {
          responseObserver.onNext(
              NessieConfiguration.newBuilder().setDefaultBranch(REF_NAME).build());
          responseObserver.onCompleted();
        }
      };

  public ServiceWithChannel startGrpcServer() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(treeService)
            .addService(configService)
            .addService(contentService)
            .intercept(serverInterceptor)
            .build();
    grpcCleanup.register(server.start());
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
    grpcCleanup.register(channel);
    return new ServiceWithChannel(server, channel);
  }

  /**
   * Server interceptor to read headers if supplied by client.
   */
  private final TestHeaderServerInterceptor serverInterceptor = new TestHeaderServerInterceptor();

  protected TestHeaderServerInterceptor getServerInterceptor() {
    return serverInterceptor;
  }

  @Before
  public void setup() {
    serverInterceptor.clear();
  }

  /**
   * ServiceWithChannel
   */
  public static class ServiceWithChannel {
    private final Server server;
    private final ManagedChannel channel;

    public ServiceWithChannel(Server server, ManagedChannel channel) {
      this.server = server;
      this.channel = channel;
    }

    public Server getServer() {
      return server;
    }

    public ManagedChannel getChannel() {
      return channel;
    }
  }

  /**
   * Simple dummy header intercepter to verify client side headers.
   */
  public static class TestHeaderServerInterceptor implements ServerInterceptor {
    private final List<String> headerValues = new ArrayList<>();

    public List<String> getHeaderValues() {
      return ImmutableList.copyOf(headerValues);
    }

    public void clear() {
      headerValues.clear();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall,
                                                                 Metadata metadata,
                                                                 ServerCallHandler<ReqT, RespT> serverCallHandler) {
      if (metadata.containsKey(TEST_HEADER_KEY)) {
        headerValues.add(metadata.get(TEST_HEADER_KEY));
      }
      return serverCallHandler.startCall(new SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
      }, metadata);
    }
  }
}
