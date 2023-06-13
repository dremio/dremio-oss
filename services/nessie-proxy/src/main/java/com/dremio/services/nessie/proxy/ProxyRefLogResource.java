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

import static com.dremio.services.nessie.proxy.ProxyUtil.paging;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.v1.http.HttpRefLogApi;
import org.projectnessie.api.v1.params.RefLogParams;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.ser.Views;

import com.fasterxml.jackson.annotation.JsonView;

/**
 * Nessie RefLog-API REST endpoint that forwards via gRPC.
 */
@RequestScoped
public class ProxyRefLogResource implements HttpRefLogApi {

  @SuppressWarnings("checkstyle:visibilityModifier")
  @Inject
  NessieApiV1 api;

  public ProxyRefLogResource() {
  }

  @Override
  @JsonView(Views.V1.class)
  public RefLogResponse getRefLog(RefLogParams params) throws NessieNotFoundException {
    GetRefLogBuilder req = api.getRefLog();
    if (params.endHash() != null) {
      req.fromHash(params.endHash());
    }
    if (params.startHash() != null) {
      req.untilHash(params.startHash());
    }
    if (params.filter() != null) {
      req.filter(params.filter());
    }
    return paging(req, params.pageToken(), params.maxRecords()).get();
  }
}
