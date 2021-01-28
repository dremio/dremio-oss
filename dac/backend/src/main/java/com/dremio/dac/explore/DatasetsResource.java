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
package com.dremio.dac.explore;

import static com.dremio.dac.explore.DatasetTool.TMP_DATASET_PATH;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.DatasetDetails;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSearchUI;
import com.dremio.dac.explore.model.DatasetSearchUIs;
import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.Home;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.resource.BaseResourceWithAllocator;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.errors.NewDatasetQueryException;
import com.dremio.dac.service.search.SearchContainer;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.file.FilePath;
import com.dremio.file.SourceFilePath;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.ExtendedConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Preconditions;

/**
 * List datasets from space/folder/home/source
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/datasets")
public class DatasetsResource extends BaseResourceWithAllocator {

  private final DatasetVersionMutator datasetService;
  private final NamespaceService namespaceService;
  private final DatasetTool tool;
  private final ConnectionReader connectionReader;
  private final DatasetCatalog datasetCatalog;
  private final CatalogServiceHelper catalogServiceHelper;

  @Inject
  public DatasetsResource(
    NamespaceService namespaceService,
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    QueryExecutor executor,
    ConnectionReader connectionReader,
    @Context SecurityContext securityContext,
    DatasetCatalog datasetCatalog,
    CatalogServiceHelper catalogServiceHelper,
    BufferAllocatorFactory allocatorFactory
      ) {
    this(namespaceService, datasetService,
      new DatasetTool(datasetService, jobsService, executor, securityContext),
      connectionReader, datasetCatalog, catalogServiceHelper, allocatorFactory);
  }

  protected DatasetsResource(NamespaceService namespaceService,
      DatasetVersionMutator datasetService,
      DatasetTool tool,
      ConnectionReader connectionReader,
      DatasetCatalog datasetCatalog,
      CatalogServiceHelper catalogServiceHelper,
      BufferAllocatorFactory allocatorFactory
      )
   {
     super(allocatorFactory);
    this.namespaceService = namespaceService;
    this.datasetService = datasetService;
    this.tool = tool;
    this.connectionReader = connectionReader;
    this.datasetCatalog = datasetCatalog;
    this.catalogServiceHelper = catalogServiceHelper;
  }

  private InitialPreviewResponse newUntitled(DatasetPath fromDatasetPath, DatasetVersion newVersion, Integer limit, String engineName)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    FromTable from = new FromTable(fromDatasetPath.toPathString());
    DatasetSummary summary = getDatasetSummary(fromDatasetPath);

    return newUntitled(from, newVersion, fromDatasetPath.toParentPathList(), summary, limit, engineName);
  }

  private InitialPreviewResponse newUntitled(FromBase from, DatasetVersion newVersion, List<String> context,
                                             DatasetSummary parentSummary, Integer limit, String engineName)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {

    return tool.newUntitled(getOrCreateAllocator("newUntitled"), from, newVersion, context, parentSummary, false, limit, engineName);
  }

  /**
   * A user clicked "new query" and then wrote a SQL query. This is the first version of the dataset we will be creating (this is a "initial commit")
   *
   * @param newVersion The version id we should use for the new version of dataset (generated by client)
   * @param sql The sql information to generate the new dataset
   * @param limit The number of records to return in the initial response
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("new_untitled_sql")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse newUnitledSql(
      @QueryParam("newVersion") DatasetVersion newVersion,
      @QueryParam("limit") Integer limit,
      /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");
    return newUntitled(new FromSQL(sql.getSql()).setAlias("nested_0"), newVersion, sql.getContext(), null, limit, sql.getEngineName());
  }

  @POST @Path("new_untitled_sql_and_run")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public InitialRunResponse newUnitledSqlAndRun(
    @QueryParam("newVersion") DatasetVersion newVersion,
      /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, InterruptedException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");

    return tool.newUntitledAndRun(new FromSQL(sql.getSql()).setAlias("nested_0"), newVersion, sql.getContext(), sql.getEngineName());
  }

  /**
   * Create a new query of SELECT * from [parentDataset].
   *
   * @param parentDataset
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("new_untitled") @Produces(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse newUntitledFromParent(
      @QueryParam("parentDataset") DatasetPath parentDataset,
      @QueryParam("newVersion") DatasetVersion newVersion,
      @QueryParam("limit") Integer limit,
      @QueryParam("engineName") String engineName)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");
    try {
      return newUntitled(parentDataset, newVersion, limit, engineName);
    } catch (DatasetNotFoundException | NamespaceException e) {
      // TODO: this should really be a separate API from the UI.
      // didn't find as virtual dataset, let's return as opaque sql (as this could be a source) .
      return newUntitled(parentDataset, newVersion, limit, engineName);
    }
  }

  public InitialPreviewResponse createUntitledFromSourceFile(SourceName sourceName, String path, Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    return tool.newUntitled(getOrCreateAllocator("createUntitledFromSourceFile"), new FromTable(filePath.toPathString()), DatasetVersion.newVersion(), filePath.toParentPathList(), limit);
  }

  public InitialPreviewResponse createUntitledFromSourceFolder(SourceName sourceName, String path, Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
    return tool.newUntitled(getOrCreateAllocator("createUntitledFromSourceFolder"), new FromTable(folderPath.toPathString()), DatasetVersion.newVersion(), folderPath.toPathList(), limit);
  }

  public InitialPreviewResponse createUntitledFromPhysicalDataset(SourceName sourceName, String path, Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    PhysicalDatasetPath datasetPath = PhysicalDatasetPath.fromURLPath(sourceName, path);
    return tool.newUntitled(getOrCreateAllocator("createUntitledFromPhysicalDataset"), new FromTable(datasetPath.toPathString()), DatasetVersion.newVersion(), datasetPath.toParentPathList(), limit);
  }

  public InitialPreviewResponse createUntitledFromHomeFile(HomeName homeName, String path, Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    FilePath filePath = FilePath.fromURLPath(homeName, path);
    return tool.newUntitled(getOrCreateAllocator("createUntitledFromHomeFile"), new FromTable(filePath.toPathString()), DatasetVersion.newVersion(), filePath.toParentPathList(), limit);
  }


  @GET
  @Path("search")
  @Produces(MediaType.APPLICATION_JSON)
  public DatasetSearchUIs searchDatasets(@QueryParam("filter") String filters, @QueryParam("sort") String sortColumn,
                                         @QueryParam("order") SortOrder order) throws NamespaceException, DatasetVersionNotFoundException {
    final DatasetSearchUIs datasets = new DatasetSearchUIs();
    for (SearchContainer searchEntity : catalogServiceHelper.searchByQuery(filters)) {
      if (searchEntity.getNamespaceContainer().getType().equals(NameSpaceContainer.Type.DATASET)) {
        datasets.add(new DatasetSearchUI(searchEntity.getNamespaceContainer().getDataset(), searchEntity.getCollaborationTag()));
      }
    }
    return datasets;
  }

  /**
   * Get summary for dataset (physical datasets, virtual datasets)
   * @param path relative path to the summary
   * @return
   * @throws NamespaceException
   */
  @GET
  @Path("/summary/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public DatasetSummary getDatasetSummary(@PathParam("path") String path) throws NamespaceException, DatasetNotFoundException {
    final DatasetPath datasetPath = new DatasetPath(PathUtils.toPathComponents(path));
    return getDatasetSummary(datasetPath);
  }

  private DatasetSummary getDatasetSummary(DatasetPath datasetPath) throws NamespaceException, DatasetNotFoundException {
    final DremioTable table = datasetCatalog.getTable(datasetPath.toNamespaceKey());
    if (table == null) {
      throw new DatasetNotFoundException(datasetPath);
    }
    final DatasetConfig datasetConfig = table.getDatasetConfig();

    return newDatasetSummary(datasetConfig,
      datasetService.getJobsCount(datasetPath.toNamespaceKey()),
      datasetService.getDescendantsCount(datasetPath.toNamespaceKey()));
  }

  protected DatasetSummary newDatasetSummary(DatasetConfig datasetConfig, int jobCount, int descendants) throws NamespaceException {
    return DatasetSummary.newInstance(datasetConfig, jobCount, descendants);
  }

  @GET
  @Path("/context/{type}/{datasetContainer}/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public DatasetDetails getDatasetContext(@PathParam("type") String type,
                                          @PathParam("datasetContainer") String datasetContainer,
                                          @PathParam("path") String path)
      throws Exception {
    // TODO - DX-4072 - this is a bit hacky, but not sure of a better way to do this right now, handling
    // of dataset paths inside of URL paths could use overall review and standardization
    final DatasetPath datasetPath = new DatasetPath(datasetContainer + "." + path);
    if (datasetPath.equals(TMP_DATASET_PATH)) {
      // TODO - this can be removed if the UI prevents sending tmp.UNTITLED, for now handle it gracefully and hand
      // back a response that will not cause a rendering failure
      return new DatasetDetails(
          TMP_DATASET_PATH.toPathList(),
          "", 0, 0, System.currentTimeMillis(),
          new Space(null, "None", null, null, null, 0, null));
    }

    final DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath.toNamespaceKey());
    String containerName = datasetConfig.getFullPathList().get(0);
    DatasetContainer spaceInfo;
    if ("home".equals(type)) {
      HomePath homePath = new HomePath(containerName);
      HomeConfig home = namespaceService.getHome(homePath.toNamespaceKey());
      long dsCount = namespaceService.getAllDatasetsCount(homePath.toNamespaceKey());
      home.setExtendedConfig(new ExtendedConfig().setDatasetCount(dsCount));
      spaceInfo = newHome(homePath, home);
    } else if ("space".equals(type)) {
      final NamespaceKey spaceKey = new SpacePath(containerName).toNamespaceKey();
      SpaceConfig space = namespaceService.getSpace(spaceKey);
      spaceInfo = newSpace(space, namespaceService.getAllDatasetsCount(spaceKey));
    } else if ("source".equals(type)) {
      final NamespaceKey sourceKey = new SourcePath(containerName).toNamespaceKey();
      SourceConfig source = namespaceService.getSource(sourceKey);
      BoundedDatasetCount datasetCount = namespaceService.getDatasetCount(sourceKey, BoundedDatasetCount.SEARCH_TIME_LIMIT_MS, BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH);
      spaceInfo = SourceUI.get(source, connectionReader)
          .setNumberOfDatasets(datasetCount.getCount());
    } else {
      throw new DACRuntimeException("Incorrect dataset container type provided:" + type);
    }
    return new DatasetDetails(datasetConfig,
        datasetService.getJobsCount(datasetPath.toNamespaceKey()),
        datasetService.getDescendantsCount(datasetPath.toNamespaceKey()),
        spaceInfo
    );
  }

  protected Home newHome(HomePath homePath, HomeConfig home) {
    return new Home(homePath, home);
  }

  protected Space newSpace(SpaceConfig spaceConfig, int datasetCount) throws Exception {
    return Space.newInstance(spaceConfig, null, datasetCount);
  }
}
