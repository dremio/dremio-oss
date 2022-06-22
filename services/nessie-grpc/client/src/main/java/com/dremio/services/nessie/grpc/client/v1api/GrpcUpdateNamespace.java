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
package com.dremio.services.nessie.grpc.client.v1api;

import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespaceParamsBuilder;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Namespace;

import com.dremio.services.nessie.grpc.api.NamespaceServiceGrpc.NamespaceServiceBlockingStub;
import com.dremio.services.nessie.grpc.api.NamespaceUpdateRequest;
import com.dremio.services.nessie.grpc.client.GrpcExceptionMapper;

public class GrpcUpdateNamespace implements UpdateNamespaceBuilder {
  private final NamespaceServiceBlockingStub stub;
  private final NamespaceParamsBuilder builder = NamespaceParams.builder();
  private final NamespaceUpdateRequest.Builder request = NamespaceUpdateRequest.newBuilder();

  GrpcUpdateNamespace(NamespaceServiceBlockingStub namespaceServiceBlockingStub) {
    stub = namespaceServiceBlockingStub;
  }

  @Override
  public UpdateNamespaceBuilder namespace(Namespace namespace) {
    builder.namespace(namespace);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    builder.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperties(Set<String> propertyRemovals) {
    request.addAllPropertyRemovals(propertyRemovals);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperty(String key, String value) {
    request.putPropertyUpdates(key, value);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperty(String key) {
    request.addPropertyRemovals(key);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperties(Map<String, String> propertyUpdates) {
    request.putAllPropertyUpdates(propertyUpdates);
    return this;
  }

  @Override
  public void update() throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    GrpcExceptionMapper.handleNamespaceRetrieval(() ->
      stub.updateProperties(request.setNamespaceRequest(toProto(builder.build())).build())
    );
  }
}
