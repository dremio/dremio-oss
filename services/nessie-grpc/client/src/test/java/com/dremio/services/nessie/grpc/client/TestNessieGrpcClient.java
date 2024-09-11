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

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.services.nessie.grpc.client.impl.GrpcApiImpl;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.URI;
import java.util.function.Function;
import org.junit.Test;
import org.mockito.Mockito;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ContentKey;

/** Tests for the {@link GrpcApiImpl} */
public class TestNessieGrpcClient extends AbstractTestNessieGrpcClient {
  interface IncompatibleApiInterface extends NessieApi {}

  @Test
  public void testIncompatibleApiInterface() {
    assertThatThrownBy(
            () ->
                GrpcClientBuilder.builder()
                    .withChannel(Mockito.mock(ManagedChannel.class))
                    .build(IncompatibleApiInterface.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "API version com.dremio.services.nessie.grpc.client.TestNessieGrpcClient$IncompatibleApiInterface is not supported.");
  }

  @Test
  public void testUnsupportedMethods() {
    assertThatThrownBy(
            () -> GrpcClientBuilder.builder().withUri((URI) null).build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("withUri is not supported for gRPC");

    assertThatThrownBy(
            () -> GrpcClientBuilder.builder().withUri("http://localhost").build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("withUri is not supported for gRPC");

    assertThatThrownBy(
            () ->
                GrpcClientBuilder.builder()
                    .fromConfig(Function.identity())
                    .build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("fromConfig is not supported for gRPC");

    assertThatThrownBy(
            () ->
                GrpcClientBuilder.builder()
                    .withAuthenticationFromConfig(Function.identity())
                    .build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("withAuthenticationFromConfig is not supported for gRPC");

    assertThatThrownBy(
            () -> GrpcClientBuilder.builder().withAuthentication(null).build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("withAuthentication is not supported for gRPC");

    assertThatThrownBy(
            () -> GrpcClientBuilder.builder().fromSystemProperties().build(NessieApiV2.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("fromSystemProperties is not supported for gRPC");
  }

  @Test
  public void testNull() {
    assertThatThrownBy(() -> GrpcClientBuilder.builder().withChannel(null).build(NessieApiV2.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Channel must be configured");
  }

  @Test
  public void testApiCalls() throws IOException {
    ServiceWithChannel serviceWithChannel = startGrpcServer();
    assertThat(serviceWithChannel.getServer().getServices()).hasSize(3);
    NessieApiV2 api =
        GrpcClientBuilder.builder()
            .withChannel(serviceWithChannel.getChannel())
            .build(NessieApiV2.class);
    assertThat(api.getConfig().getDefaultBranch()).isEqualTo(REF_NAME);
    assertThat(api.getDefaultBranch().getName()).isEqualTo(REF_NAME);
    assertThat(api.getAllReferences().get().getReferences()).containsExactly(REF);
    assertThat(api.getContent().key(ContentKey.of("test")).refName(REF_NAME).get())
        .containsValue(fromProto(ICEBERG_TABLE));
  }

  @Test
  public void testApiV2Calls() throws IOException {
    ServiceWithChannel serviceWithChannel = startGrpcServer();
    assertThat(serviceWithChannel.getServer().getServices()).hasSize(3);
    NessieApiV2 api =
        GrpcClientBuilder.builder()
            .withChannel(serviceWithChannel.getChannel())
            .build(NessieApiV2.class);
    assertThat(api.getConfig().getDefaultBranch()).isEqualTo(REF_NAME);
    assertThat(api.getDefaultBranch().getName()).isEqualTo(REF_NAME);
    assertThat(api.getAllReferences().get().getReferences()).containsExactly(REF);
    assertThat(api.getContent().key(ContentKey.of("test")).refName(REF_NAME).get())
        .containsValue(fromProto(ICEBERG_TABLE));
  }

  @Test
  public void testApiCallsWithInterceptor() throws IOException {
    ServiceWithChannel serviceWithChannel = startGrpcServer();
    assertThat(serviceWithChannel.getServer().getServices()).hasSize(3);
    Metadata clientHeaders = new Metadata();
    String headerValue = "some-dummy-value";
    clientHeaders.put(TEST_HEADER_KEY, headerValue);
    NessieApiV2 api =
        GrpcClientBuilder.builder()
            .withChannel(serviceWithChannel.getChannel())
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(clientHeaders))
            .build(NessieApiV2.class);
    assertThat(api.getConfig().getDefaultBranch()).isEqualTo(REF_NAME);
    assertThat(api.getDefaultBranch().getName()).isEqualTo(REF_NAME);
    assertThat(api.getAllReferences().get().getReferences()).containsExactly(REF);
    assertThat(api.getContent().key(ContentKey.of("test")).refName(REF_NAME).get())
        .containsValue(fromProto(ICEBERG_TABLE));
    assertThat(getServerInterceptor().getHeaderValues())
        .containsExactly(headerValue, headerValue, headerValue, headerValue);
  }
}
