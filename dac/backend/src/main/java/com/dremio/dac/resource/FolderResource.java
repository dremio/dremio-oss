/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.Arrays;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.FolderNotFoundException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.space.proto.ExtendedConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;

/**
 * Rest resource for spaces.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/space/{space}")
public class FolderResource {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FolderResource.class);

  private final DatasetVersionMutator datasetService;
  private final NamespaceService namespaceService;
  private final SpaceName spaceName;

  @Inject
  public FolderResource(
      DatasetVersionMutator datasetService,
      NamespaceService namespaceService,
      @PathParam("space") SpaceName spaceName) {
    this.datasetService = datasetService;
    this.namespaceService = namespaceService;
    this.spaceName = spaceName;
  }

  @GET
  @Path("/folder/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Folder getFolder(@PathParam("path") String path, @QueryParam("includeContents") @DefaultValue("true") boolean includeContents) throws NamespaceException, FolderNotFoundException, DatasetNotFoundException {
    FolderPath folderPath = FolderPath.fromURLPath(spaceName, path);
    try {
      final FolderConfig folderConfig = namespaceService.getFolder(folderPath.toNamespaceKey());
      final List<NamespaceKey> datasetPaths = namespaceService.getAllDatasets(folderPath.toNamespaceKey());
      final ExtendedConfig extendedConfig = new ExtendedConfig().setDatasetCount((long)datasetPaths.size())
        .setJobCount(datasetService.getJobsCount(datasetPaths));
      folderConfig.setExtendedConfig(extendedConfig);
      NamespaceTree contents = includeContents
          ? newNamespaceTree(namespaceService.list(folderPath.toNamespaceKey()))
          : null;
      return newFolder(folderPath, folderConfig, contents);
    } catch (NamespaceNotFoundException nfe) {
      throw new FolderNotFoundException(folderPath, nfe);
    }
  }

  @DELETE
  @Path("/folder/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteFolder(@PathParam("path") String path, @QueryParam("version") Long version) throws NamespaceException, FolderNotFoundException {
    FolderPath folderPath = FolderPath.fromURLPath(spaceName, path);
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }
    try {
      namespaceService.deleteFolder(folderPath.toNamespaceKey(), version);
    } catch (NamespaceNotFoundException nfe) {
      throw new FolderNotFoundException(folderPath, nfe);
    }
  }

  @POST
  @Path("/folder/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Folder createFolder(FolderName name, @PathParam("path") String path) throws NamespaceException  {
    String fullPath = PathUtils.toFSPathString(Arrays.asList(path, name.toString()));
    FolderPath folderPath = FolderPath.fromURLPath(spaceName, fullPath);

    final FolderConfig folderConfig = new FolderConfig();
    folderConfig.setFullPathList(folderPath.toPathList());
    folderConfig.setName(folderPath.getFolderName().getName());
    try {
      namespaceService.addOrUpdateFolder(folderPath.toNamespaceKey(), folderConfig);
    } catch(NamespaceNotFoundException nfe) {
      throw new ClientErrorException("Parent folder doesn't exist", nfe);
    }

    return newFolder(folderPath, folderConfig, null);
  }

  protected Folder newFolder(FolderPath folderPath, FolderConfig folderConfig, NamespaceTree contents) throws NamespaceNotFoundException {
    return Folder.newInstance(folderPath, folderConfig, contents, false, false);
  }

  protected NamespaceTree newNamespaceTree(List<NameSpaceContainer> children) throws DatasetNotFoundException, NamespaceException {
    return NamespaceTree.newInstance(datasetService, namespaceService, children, SPACE);
  }

  @POST
  @Produces(APPLICATION_JSON)
  @Path("/rename_folder/{path: .*}")
  public Folder renameFolder(@PathParam("path") String path, @QueryParam("renameTo") String renameTo)
      throws NamespaceException, FolderNotFoundException {
    throw UserException.unsupportedError()
        .message("Renaming a folder is not supported")
        .build(logger);
  }

}
