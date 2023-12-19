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
package com.dremio.dac.resource;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.ser.Views;

import com.dremio.services.nessie.proxy.ProxyV2TreeResource;
import com.fasterxml.jackson.annotation.JsonView;

/**
 * Nessie-specific extension of {@link ProxyV2TreeResource}.
 * Disables certain API calls that are not needed in the NaaS proxy.
 */

public class V2TreeResource extends ProxyV2TreeResource {

  @Inject
  public V2TreeResource(NessieApiV2 api) {
    super(api);
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse assignReference(String typeName, String ref, Reference assignTo) {
    throw new WebApplicationException("assignReference is not supported", Response.Status.FORBIDDEN);
  }

  @Override
  @JsonView(Views.V2.class)
  public MergeResponse transplantCommitsIntoBranch(String branch, Transplant transplant) throws NessieConflictException, NessieNotFoundException {
    throw new WebApplicationException("transplantCommitsIntoBranch is not supported", Response.Status.FORBIDDEN);
  }

  @Override
  @JsonView(Views.V2.class)
  public CommitResponse commitMultipleOperations(String branch, Operations operations) throws NessieConflictException, NessieNotFoundException {
    throw new WebApplicationException("commitMultipleOperations is not supported", Response.Status.FORBIDDEN);
  }
}
