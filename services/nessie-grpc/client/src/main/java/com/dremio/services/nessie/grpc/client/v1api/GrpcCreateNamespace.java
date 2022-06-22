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

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNamespaceCreation;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespaceParamsBuilder;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;

import com.dremio.services.nessie.grpc.api.NamespaceServiceGrpc.NamespaceServiceBlockingStub;

public class GrpcCreateNamespace implements CreateNamespaceBuilder {

  private final NamespaceServiceBlockingStub stub;
  private final NamespaceParamsBuilder builder = NamespaceParams.builder();
  private final Map<String, String> properties = new HashMap<>();
  private Namespace namespace;

  GrpcCreateNamespace(NamespaceServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public CreateNamespaceBuilder namespace(Namespace namespace) {
    this.namespace = namespace;
    return this;
  }

  @Override
  public CreateNamespaceBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public CreateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    builder.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public CreateNamespaceBuilder properties(Map<String, String> properties) {
    this.properties.putAll(properties);
    return this;
  }

  @Override
  public CreateNamespaceBuilder property(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  @Override
  public Namespace create()
    throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    return handleNamespaceCreation(() -> {
      builder.namespace(ImmutableNamespace.builder()
        .from(this.namespace)
        .properties(properties)
        .build());
      return fromProto(stub.createNamespace(toProto(builder.build())));
    });
  }
}
