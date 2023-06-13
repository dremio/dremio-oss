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

import java.util.Map;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.v1.http.HttpContentApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.ImmutableGetMultipleContentsResponse;
import org.projectnessie.model.ser.Views;

import com.fasterxml.jackson.annotation.JsonView;

/** Nessie content-API REST endpoint that forwards via gRPC. */
@RequestScoped
public class ProxyContentResource implements HttpContentApi {

  @SuppressWarnings("checkstyle:visibilityModifier")
  @Inject NessieApiV1 api;

  public ProxyContentResource() {
  }

  @Override
  @JsonView(Views.V1.class)
  public Content getContent(ContentKey key,
    String ref, String hashOnRef) throws NessieNotFoundException {
    Content content = api.getContent().refName(ref).hashOnRef(hashOnRef).key(key).get().get(key);
    if (content == null) {
      throw new NessieContentNotFoundException(key, ref);
    }
    return content;
  }

  @Override
  @JsonView(Views.V1.class)
  public GetMultipleContentsResponse getMultipleContents(String ref,
    String hashOnRef, GetMultipleContentsRequest request) throws NessieNotFoundException {
    Map<ContentKey, Content> contents = api.getContent().refName(ref).hashOnRef(hashOnRef)
      .keys(request.getRequestedKeys()).get();
    ImmutableGetMultipleContentsResponse.Builder resp = ImmutableGetMultipleContentsResponse.builder();
    contents.forEach((k, v) -> resp.addContents(ContentWithKey.of(k, v)));
    return resp.build();
  }
}
