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
package com.dremio.services.nessie.proxy;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.v1.http.HttpNamespaceApi;
import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.NamespaceUpdate;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.client.api.DeleteNamespaceBuilder;
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.client.api.GetNamespaceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.ser.Views;

import com.fasterxml.jackson.annotation.JsonView;

/**
 * Nessie Namespace-API REST endpoint that forwards via gRPC.
 */
@RequestScoped
public class ProxyNamespaceResource implements HttpNamespaceApi {

  @SuppressWarnings("checkstyle:visibilityModifier")
  @Inject
  NessieApiV1 api;

  public ProxyNamespaceResource() {
  }

  @Override
  @JsonView(Views.V1.class)
  public Namespace createNamespace(NamespaceParams params, Namespace namespace)
    throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    CreateNamespaceBuilder builder = api.createNamespace().refName(params.getRefName())
      .namespace(params.getNamespace())
      .properties(namespace.getProperties());
    if (null != params.getHashOnRef()) {
      builder.hashOnRef(params.getHashOnRef());
    }
    return builder.create();
  }

  @Override
  @JsonView(Views.V1.class)
  public void deleteNamespace(NamespaceParams params)
    throws NessieReferenceNotFoundException, NessieNamespaceNotEmptyException, NessieNamespaceNotFoundException {
    DeleteNamespaceBuilder builder = api.deleteNamespace().refName(params.getRefName())
      .namespace(params.getNamespace());
    if (null != params.getHashOnRef()) {
      builder.hashOnRef(params.getHashOnRef());
    }
    builder.delete();
  }

  @Override
  @JsonView(Views.V1.class)
  public Namespace getNamespace(NamespaceParams params)
    throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    GetNamespaceBuilder builder = api.getNamespace().refName(params.getRefName())
      .namespace(params.getNamespace());
    if (null != params.getHashOnRef()) {
      builder.hashOnRef(params.getHashOnRef());
    }
    return builder.get();
  }

  @Override
  @JsonView(Views.V1.class)
  public GetNamespacesResponse getNamespaces(MultipleNamespacesParams params)
    throws NessieReferenceNotFoundException {
    GetMultipleNamespacesBuilder builder = api.getMultipleNamespaces()
      .refName(params.getRefName());
    if (null != params.getNamespace()) {
      builder.namespace(params.getNamespace());
    }
    if (null != params.getHashOnRef()) {
      builder.hashOnRef(params.getHashOnRef());
    }
    return builder.get();
  }

  @Override
  @JsonView(Views.V1.class)
  public void updateProperties(NamespaceParams params, NamespaceUpdate namespaceUpdate)
    throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    UpdateNamespaceBuilder builder = api.updateProperties().refName(params.getRefName())
      .namespace(params.getNamespace());

    if (namespaceUpdate.getPropertyRemovals() != null) {
      builder.removeProperties(namespaceUpdate.getPropertyRemovals());
    }

    if (namespaceUpdate.getPropertyUpdates() != null) {
      builder.updateProperties(namespaceUpdate.getPropertyUpdates());
    }

    if (null != params.getHashOnRef()) {
      builder.hashOnRef(params.getHashOnRef());
    }
    builder.update();
  }
}
