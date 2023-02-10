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

import java.util.Map;

import javax.ws.rs.core.Response;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.DatasetResourceUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Base class for resources that are for loading datasets in BI Tools.
 */
public abstract class BaseBIToolResource {
  private final ProjectOptionManager optionManager;
  private final DatasetCatalog datasetCatalog;
  private final DatasetPath path;
  private final Map<String, VersionContextReq> references;

  protected BaseBIToolResource(ProjectOptionManager optionManager, DatasetCatalog datasetCatalog,
                               String path, String refType, String refValue) {
    this.optionManager = optionManager;
    this.datasetCatalog = datasetCatalog;
    this.path = new DatasetPath(PathUtils.toPathComponents(path));
    this.references = DatasetResourceUtils.createSourceVersionMapping(this.path.getRoot().getName(), refType, refValue);
  }

  /**
   * Returns a response providing a way to connect to the given dataset on a given host with
   * the BI tool associated with this resource
   * @param host The host to specify to the BI tool. Can be null.
   * @return A response providing a way to load the given dataset in the BI tool.
   */
  protected Response getWithHostHelper(String host) throws DatasetNotFoundException {
    // Check the endpoint is enabled.
    final TypeValidators.BooleanValidator endpointEnabledOption = getClientToolOption();
    if (endpointEnabledOption != null) {
      if (!optionManager.isSet(endpointEnabledOption.getOptionName())) {
        optionManager.setOption(endpointEnabledOption.getDefault());
      }

      if (!optionManager.getOption(endpointEnabledOption)) {
        return Response.status(Response.Status.FORBIDDEN).build();
      }
    }

    // Make sure path exists
    final DatasetCatalog datasetNewCatalog =
      datasetCatalog.resolveCatalog(DatasetResourceUtils.createSourceVersionMapping(references));
    final DremioTable table = datasetNewCatalog.getTable(path.toNamespaceKey());
    if (table == null) {
      throw new DatasetNotFoundException(path);
    }
    final DatasetConfig datasetConfig = table.getDatasetConfig();
    final Response.ResponseBuilder builder =  Response.ok().entity(datasetConfig);
    return buildResponseWithHost(builder, host).build();
  }

  /**
   * Returns a response with the correct hostname header if appropriate.
   * @param builder The response builder to populate.
   * @param host The host to specify to the BI tool. Can be null.
   * @return A response builder with the correct hostname header.
   */
  @VisibleForTesting
  Response.ResponseBuilder buildResponseWithHost(Response.ResponseBuilder builder, String host) {
    if (host == null) {
      return builder;
    }

    final String hostOnly;
    final int portIndex = host.indexOf(":");
    if (portIndex == -1) {
      hostOnly = host;
    } else {
      hostOnly = host.substring(0, portIndex);
    }

    return builder.header(WebServer.X_DREMIO_HOSTNAME, hostOnly);
  }

  /**
   * Get the option to use to verify this endpoint is enabled, or null if this
   * endpoint is always enabled.
   */
  protected abstract TypeValidators.BooleanValidator getClientToolOption();
}
