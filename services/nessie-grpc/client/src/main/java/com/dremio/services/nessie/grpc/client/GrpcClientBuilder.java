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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.auth.NessieAuthentication;

import com.dremio.services.nessie.grpc.client.v1api.GrpcApiV1Impl;
import com.google.common.base.Preconditions;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;

/**
 * A builder class that creates a {@link NessieApi} via {@link GrpcClientBuilder#builder()}. Note
 * that the user is responsible for closing the {@link ManagedChannel} instance when it is being
 * provided via {@link GrpcClientBuilder#withChannel(ManagedChannel)}, unless {@link
 * GrpcClientBuilder#shutdownChannel(boolean)} is set to <code>true</code>.
 */
public final class GrpcClientBuilder implements NessieClientBuilder<GrpcClientBuilder> {

  private GrpcClientBuilder() {
  }

  private ManagedChannel channel;
  private boolean shutdownChannel = false;
  private List<ClientInterceptor> clientInterceptors = new ArrayList<>();

  /**
   * Returns a new {@link GrpcClientBuilder} instance.
   *
   * @return A new {@link GrpcClientBuilder} instance.
   */
  public static GrpcClientBuilder builder() {
    return new GrpcClientBuilder();
  }

  /**
   * Sets the {@link ManagedChannel} to use when connecting to the gRPC server.
   *
   * @param channel The {@link ManagedChannel} to use when connecting to the gRPC server.
   * @return {@code this}
   */
  public GrpcClientBuilder withChannel(ManagedChannel channel) {
    this.channel = channel;
    return this;
  }

  /**
   * Configures whether the provided channel should be automatically shut down when the {@link
   * NessieApi} instance is closed.
   *
   * @param shutdownChannel if set to <code>true</code>, the provided channel will be shut down when
   *                        the {@link NessieApi} instance is closed.
   * @return {@code this}
   */
  public GrpcClientBuilder shutdownChannel(boolean shutdownChannel) {
    this.shutdownChannel = shutdownChannel;
    return this;
  }

  /**
   * Adds the provided {@link ClientInterceptor}s to the stubs generated when making calls to the gRPC server.
   *
   * @param clientInterceptors The {@link ClientInterceptor}s to use when make calss to the gRPC server.
   * @return {@code this}
   */
  public GrpcClientBuilder withInterceptors(ClientInterceptor...clientInterceptors) {
    this.clientInterceptors.clear();
    this.clientInterceptors.addAll(Arrays.asList(clientInterceptors));
    return this;
  }

  @Override
  public GrpcClientBuilder fromSystemProperties() {
    throw new UnsupportedOperationException("fromSystemProperties is not supported for gRPC");
  }

  @Override
  public GrpcClientBuilder fromConfig(Function<String, String> configuration) {
    throw new UnsupportedOperationException("fromConfig is not supported for gRPC");
  }

  @Override
  public GrpcClientBuilder withAuthenticationFromConfig(Function<String, String> configuration) {
    throw new UnsupportedOperationException(
      "withAuthenticationFromConfig is not supported for gRPC");
  }

  @Override
  public GrpcClientBuilder withAuthentication(NessieAuthentication authentication) {
    throw new UnsupportedOperationException("withAuthentication is not supported for gRPC");
  }

  @Override
  public GrpcClientBuilder withUri(URI uri) {
    throw new UnsupportedOperationException("withUri is not supported for gRPC");
  }

  @Override
  public GrpcClientBuilder withUri(String uri) {
    throw new UnsupportedOperationException("withUri is not supported for gRPC");
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public <API extends NessieApi> API build(Class<API> apiVersion) {
    Objects.requireNonNull(apiVersion, "API version class must be non-null");
    Preconditions.checkArgument(channel != null, "Channel must be configured");

    if (apiVersion.isAssignableFrom(NessieApiV1.class)) {
      Preconditions.checkArgument(apiVersion.isInterface(),
        "must not use a concrete class for the apiVersion parameter");
      return (API) new GrpcApiV1Impl(channel, shutdownChannel, clientInterceptors.toArray(new ClientInterceptor[0]));
    }

    throw new IllegalArgumentException(
      String.format("API version %s is not supported.", apiVersion.getName()));
  }
}
