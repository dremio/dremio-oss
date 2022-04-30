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

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;

import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.resourcetree.ResourceList;
import com.dremio.dac.model.resourcetree.ResourceTreeEntity;
import com.dremio.dac.model.resourcetree.ResourceTreeSourceEntity;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.Lists;

/**
 * Rest resource for resource tree.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/resourcetree")
public class ResourceTreeResource {
  private final Provider<NamespaceService> namespaceService;
  private final SecurityContext securityContext;
  private final SourceService sourceService;
  private final SabotContext context;
  private final ConnectionReader connectionReader;

  @Inject
  public ResourceTreeResource(
    Provider<NamespaceService> namespaceService,
    @Context SecurityContext securityContext,
    SourceService sourceService, SabotContext context, ConnectionReader connectionReader) {
    this.namespaceService = namespaceService;
    this.securityContext = securityContext;
    this.sourceService = sourceService;
    this.context = context;
    this.connectionReader = connectionReader;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceList getResourceTree(@QueryParam("showSpaces") boolean showSpaces,
                                      @QueryParam("showSources") boolean showSources,
                                      @QueryParam("showHomes") boolean showHomes)
    throws NamespaceException, UnsupportedEncodingException {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    if (showSpaces) {
      resources.addAll(getSpaces());
    }
    if (showSources) {
      resources.addAll(getSources());
    }
    if (showHomes) {
      resources.addAll(getHomes(false));
    }
    return new ResourceList(resources);
  }

  @GET
  @Path("{rootPath}")
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceList getResources(
      @PathParam("rootPath") String rootPath,
      @QueryParam("showDatasets") boolean showDatasets,
      @QueryParam("refType") String refType,
      @QueryParam("refValue") String refValue)
    throws NamespaceException, UnsupportedEncodingException {
    final FolderPath folderPath = new FolderPath(rootPath);

    return new ResourceList(
      listPath(
        folderPath.toNamespaceKey(),
        showDatasets,
        refType,
        refValue));
  }

  @GET
  @Path("/{rootPath}/expand")
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceList getResourceTreeWithExpansion(@PathParam("rootPath") String rootPath,
                                                   @QueryParam("showSpaces") boolean showSpaces,
                                                   @QueryParam("showSources") boolean showSources,
                                                   @QueryParam("showDatasets") boolean showDatasets,
                                                   @QueryParam("showHomes") boolean showHomes)
    throws NamespaceException, UnsupportedEncodingException {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    final FolderPath folderPath = new FolderPath(rootPath);
    final List<String> expandPathList = folderPath.toPathList();
    ResourceTreeEntity root = null;

    if (showSpaces) {
      for (ResourceTreeEntity resourceTreeEntity : getSpaces()) {
        resources.add(resourceTreeEntity);
        if (root == null && resourceTreeEntity.getName().equals(expandPathList.get(0))) {
          root = resourceTreeEntity;
        }
      }
    }

    if (showSources) {
      for (ResourceTreeEntity resourceTreeEntity : getSources()) {
        resources.add(resourceTreeEntity);
        if (root == null && resourceTreeEntity.getName().equals(expandPathList.get(0))) {
          root = resourceTreeEntity;
        }
      }
    }

    if (showHomes) {
      for (ResourceTreeEntity resourceTreeEntity : getHomes(false)) {
        resources.add(resourceTreeEntity);
        if (root == null && resourceTreeEntity.getName().equalsIgnoreCase(expandPathList.get(0))) {
          root = resourceTreeEntity;
        }
      }
    }

    // see if root is space
    if (root == null) {
      try {
        root = new ResourceTreeEntity(namespaceService.get().getSpace(new NamespaceKey(expandPathList.get(0))));
        resources.add(root);
      } catch (NamespaceException nse) {
        // ignore
      }
    }

    // see if root is home
    if (root == null) {
      try {
        root = new ResourceTreeEntity(namespaceService.get().getHome(new NamespaceKey(expandPathList.get(0))));
        resources.add(root);
      } catch (NamespaceException nse) {
        // ignore
      }
    }

    if (root == null) {
      try {
        SourceConfig sourceConfig = namespaceService.get().getSource(new NamespaceKey(expandPathList.get(0)));
        if (!SourceUI.isInternal(sourceConfig, connectionReader)) {
          root = new ResourceTreeEntity(sourceConfig);
          resources.add(root);
        }
      } catch (NamespaceException nse) {
        // ignore
      }
    }

    // expand path starting from root entity.
    for (int i = 0; i < expandPathList.size() - 1; ++i) {
      NamespaceKey parentPath = new NamespaceKey(expandPathList.subList(0, i + 1));
      List<ResourceTreeEntity> intermediateResources = listPath(parentPath, showDatasets, null, null);
      if (!intermediateResources.isEmpty()) {
        root.expand(intermediateResources);
        // reset root
        for (ResourceTreeEntity resourceTreeEntity : intermediateResources) {
          if (resourceTreeEntity.getName().equalsIgnoreCase(expandPathList.get(i + 1))) {
            root = resourceTreeEntity;
            break;
          }
        }
      } else {
        // parent is empty stop exploring.
        return new ResourceList(resources);
      }
    }

    if (root == null) {
      throw new RuntimeException(rootPath + " not found");
    }
    // expand last if its not a leaf.
    if (root.isListable()) {
      root.expand(listPath(new NamespaceKey(expandPathList), showDatasets, null, null));
    }
    return new ResourceList(resources);
  }

  public List<ResourceTreeEntity> listPath(
      NamespaceKey path,
      boolean showDatasets,
      String refType,
      String refValue)
      throws NamespaceException, UnsupportedEncodingException {
    if (!path.getPathComponents().isEmpty()
        && namespaceService.get().exists(new NamespaceKey(path.getRoot()), SOURCE)) {
      // For SOURCE type, use source service directly
      return sourceService.listPath(path, showDatasets, refType, refValue);
    }

    final List<ResourceTreeEntity> resources = Lists.newArrayList();

    for (NameSpaceContainer container : namespaceService.get().list(path)) {
      if (container.getType() == Type.FOLDER) {
        resources.add(new ResourceTreeEntity(container.getFolder()));
      } else if (showDatasets && container.getType() == Type.DATASET) {
        resources.add(new ResourceTreeEntity(container.getDataset()));
      }
    }

    return resources;
  }

  public List<ResourceTreeEntity>  getSpaces() throws NamespaceException, UnsupportedEncodingException  {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    for (SpaceConfig spaceConfig : namespaceService.get().getSpaces()) {
      resources.add(new ResourceTreeEntity(spaceConfig));
    }
    return resources;
  }

  public List<ResourceTreeEntity> getHomes(boolean all) throws NamespaceException, UnsupportedEncodingException  {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    if (all) {
      for (HomeConfig homeConfig : namespaceService.get().getHomeSpaces()) {
        resources.add(new ResourceTreeEntity(homeConfig));
      }
    } else {
      resources.add(new ResourceTreeEntity(namespaceService.get().getHome(
        new NamespaceKey(HomeName.getUserHomePath(securityContext.getUserPrincipal().getName()).getName()))));
    }
    return resources;
  }

  public List<ResourceTreeEntity> getSources() throws NamespaceException, UnsupportedEncodingException  {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    for (SourceConfig sourceConfig : sourceService.getSources()) {
      resources.add(new ResourceTreeSourceEntity(sourceConfig, sourceService.getSourceState(sourceConfig.getName())));
    }
    return resources;
  }

}
