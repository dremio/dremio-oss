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
package com.dremio.dac.explore.bi;

import static com.dremio.dac.server.WebServer.MediaType.TEXT_PLAIN_QLIK_APP;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Resource to create qlik load script for a given dataset
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/qlik/{datasetPath}")
public class QlikResource {
  private final NamespaceService namespaceService;
  private final DatasetPath datasetPath;

  @Inject
  public QlikResource(
      NamespaceService namespaceService,
      @PathParam("datasetPath") DatasetPath datasetPath) {
    this.namespaceService = namespaceService;
    this.datasetPath = datasetPath;
  }

  /**
   * returns a Qlik load script for the dataset
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @GET
  @Produces(TEXT_PLAIN_QLIK_APP)
  public DatasetConfig get() throws DatasetNotFoundException, NamespaceException {
    return namespaceService.getDataset(datasetPath.toNamespaceKey());
  }
}
