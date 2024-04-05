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

import com.fasterxml.jackson.annotation.JsonView;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Path;
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.ser.Views;

/** Nessie diff-API REST endpoint that forwards via gRPC. */
@RequestScoped
@Path("api/v1/diffs")
public class ProxyDiffResource implements HttpDiffApi {

  @SuppressWarnings("checkstyle:visibilityModifier")
  @Inject
  NessieApiV1 api;

  public ProxyDiffResource() {}

  @Override
  @JsonView(Views.V1.class)
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    GetDiffBuilder diff =
        api.getDiff().fromRefName(params.getFromRef()).toRefName(params.getToRef());
    if (params.getFromHashOnRef() != null) {
      diff.fromHashOnRef(params.getFromHashOnRef());
    }
    if (params.getToHashOnRef() != null) {
      diff.toHashOnRef(params.getToHashOnRef());
    }
    return diff.get();
  }
}
