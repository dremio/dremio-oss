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

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SPACE;

import java.util.ConcurrentModificationException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetResourcePath;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.FileNotFoundException;
import com.dremio.dac.service.errors.SpaceNotFoundException;
import com.dremio.dac.util.ResourceUtil;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Rest resource for spaces.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/space/{spaceName}")
public class SpaceResource {
  private static final Logger logger = LoggerFactory.getLogger(SpaceResource.class);

  private final NamespaceService namespaceService;
  private final DatasetVersionMutator datasetService;
  private final CollaborationHelper collaborationService;
  private final SpaceName spaceName;
  private final SpacePath spacePath;

  @Inject
  public SpaceResource(
      NamespaceService namespaceService,
      DatasetVersionMutator datasetService,
      CollaborationHelper collaborationService,
      @PathParam("spaceName") SpaceName spaceName) {
    this.namespaceService = namespaceService;
    this.datasetService = datasetService;
    this.collaborationService = collaborationService;
    this.spaceName = spaceName;
    this.spacePath = new SpacePath(spaceName);
  }

  protected Space newSpace(SpaceConfig spaceConfig, NamespaceTree contents, int datasetCount) throws Exception {
    return Space.newInstance(spaceConfig, contents, datasetCount);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Space getSpace(@QueryParam("includeContents") @DefaultValue("true") boolean includeContents)
      throws Exception {
    try {
      final SpaceConfig config = namespaceService.getSpace(spacePath.toNamespaceKey());
      final int datasetCount = namespaceService.getDatasetCount(spacePath.toNamespaceKey(), BoundedDatasetCount.SEARCH_TIME_LIMIT_MS, BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH).getCount();
      NamespaceTree contents = includeContents ? newNamespaceTree(namespaceService.list(spacePath.toNamespaceKey())) : null;
      final Space space = newSpace(config, contents, datasetCount);

      return space;
    } catch (NamespaceNotFoundException nfe) {
      throw new SpaceNotFoundException(spacePath.getSpaceName().getName(), nfe);
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public void deleteSpace(@QueryParam("version") String version) throws NamespaceException, SpaceNotFoundException, UserException {
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }
    try {
      namespaceService.deleteSpace(spacePath.toNamespaceKey(), version);
    } catch (NamespaceNotFoundException nfe) {
      throw new SpaceNotFoundException(spacePath.getSpaceName().getName(), nfe);
    } catch (ConcurrentModificationException e) {
      throw ResourceUtil.correctBadVersionErrorMessage(e, "space", spaceName.getName());
    }
  }

  @POST
  @Path("/rename")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // UI does not allow to rename a space
  public Space renameSpace(@QueryParam("renameTo") String renameTo)
    throws NamespaceException, SpaceNotFoundException {
    throw UserException.unsupportedError()
        .message("Renaming a space is not supported")
        .build(logger);
  }

  @GET
  @Path("dataset/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Dataset getDataset(@PathParam("path") String path)
    throws NamespaceException, FileNotFoundException, DatasetNotFoundException {
    DatasetPath datasetPath = DatasetPath.fromURLPath(spaceName, path);
    final DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath.toNamespaceKey());
    final VirtualDatasetUI vds = datasetService.get(datasetPath, datasetConfig.getVirtualDataset().getVersion());
    return Dataset.newInstance(
      new DatasetResourcePath(datasetPath),
      new DatasetVersionResourcePath(datasetPath, vds.getVersion()),
      datasetPath.getDataset(),
      vds.getSql(),
      vds,
      datasetService.getJobsCount(datasetPath.toNamespaceKey()),
      null
    );
  }

  protected NamespaceTree newNamespaceTree(List<NameSpaceContainer> children) throws DatasetNotFoundException, NamespaceException {
    return NamespaceTree.newInstance(datasetService, children, SPACE, collaborationService);
  }
}
