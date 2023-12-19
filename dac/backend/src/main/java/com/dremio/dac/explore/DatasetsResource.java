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

import java.security.AccessControlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSearchUI;
import com.dremio.dac.explore.model.DatasetSearchUIs;
import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.explore.model.InitialUntitledRunResponse;
import com.dremio.dac.explore.model.NewUntitledFromParentRequest;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.resource.BaseResourceWithAllocator;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.errors.NewDatasetQueryException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchContainer;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.parser.TableDefinitionGenerator;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.file.FilePath;
import com.dremio.options.OptionManager;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;

/**
 * Creates datasets from SQL Runner
 * Searches datasets from Catalog
 * Provides dataset summary from Catalog
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/datasets")
public class DatasetsResource extends BaseResourceWithAllocator {
  private final DatasetVersionMutator datasetService;
  private final DatasetTool tool;
  private final DatasetCatalog datasetCatalog;
  private final CatalogServiceHelper catalogServiceHelper;
  private final CollaborationHelper collaborationService;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final UserService userService;
  private final OptionManager optionManager;

  @Inject
  public DatasetsResource(
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    QueryExecutor executor,
    @Context SecurityContext securityContext,
    DatasetCatalog datasetCatalog,
    CatalogServiceHelper catalogServiceHelper,
    BufferAllocatorFactory allocatorFactory,
    CollaborationHelper collaborationService,
    ReflectionServiceHelper reflectionServiceHelper,
    UserService userService,
    OptionManager optionManager) {
    this(datasetService,
      new DatasetTool(datasetService, jobsService, executor, securityContext),
      datasetCatalog, catalogServiceHelper, allocatorFactory, collaborationService, reflectionServiceHelper, userService, optionManager);
  }

  protected DatasetsResource(
      DatasetVersionMutator datasetService,
      DatasetTool tool,
      DatasetCatalog datasetCatalog,
      CatalogServiceHelper catalogServiceHelper,
      BufferAllocatorFactory allocatorFactory,
      CollaborationHelper collaborationService,
      ReflectionServiceHelper reflectionServiceHelper,
      UserService userService,
      OptionManager optionManager
      )
   {
     super(allocatorFactory);
    this.datasetService = datasetService;
    this.tool = tool;
    this.datasetCatalog = datasetCatalog;
    this.catalogServiceHelper = catalogServiceHelper;
    this.collaborationService = collaborationService;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.userService = userService;
    this.optionManager = optionManager;
  }

  protected OptionManager getOptionManager() {
    return optionManager;
  }

  private DremioTable getTable(DatasetPath datasetPath, Map<String, VersionContextReq> references) {
    DatasetCatalog datasetNewCatalog = datasetCatalog.resolveCatalog(DatasetResourceUtils.createSourceVersionMapping(references));
    NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final DremioTable table = datasetNewCatalog.getTable(namespaceKey);
    if (table == null) {
      throw new DatasetNotFoundException(datasetPath);
    }
    return table;
  }

  private DatasetSummary getDatasetSummary(DatasetPath datasetPath,
                                           Map<String, VersionContextReq> references) throws NamespaceException, DatasetNotFoundException {
    NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final DremioTable table = getTable(datasetPath, references);
    final DatasetConfig datasetConfig = table.getDatasetConfig();

    return newDatasetSummary(datasetConfig,
      datasetService.getJobsCount(namespaceKey),
      datasetService.getDescendantsCount(namespaceKey),
      references,
      Collections.emptyList(),
      null,
      null,
      null);
  }

  private InitialPreviewResponse newUntitled(DatasetPath fromDatasetPath,
                                             DatasetVersion newVersion,
                                             Integer limit,
                                             String engineName,
                                             String sessionId,
                                             Map<String, VersionContextReq> references,
                                             String triggerJob)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    FromTable from = new FromTable(fromDatasetPath.toPathString());
    if (DatasetTool.shouldTriggerJob(triggerJob)) {
      DatasetSummary summary = getDatasetSummary(fromDatasetPath, references);
      return newUntitled(from, newVersion, fromDatasetPath.toParentPathList(), summary, limit, engineName, sessionId, references);
    } else {
      DremioTable table = getTable(fromDatasetPath, references);
      DatasetConfig datasetConfig = table.getDatasetConfig();

      String sql;
      if (!CatalogUtil.isDatasetTypeATable(datasetConfig.getType())) {
        sql = null;
      } else {
        NamespaceKey path = fromDatasetPath.toNamespaceKey();
        Map<String, VersionContext> sourceVersion = DatasetResourceUtils.createSourceVersionMapping(references);
        VersionContext versionContext = sourceVersion.get(path.getRoot());

        sql = getTableDefinition(
          datasetConfig,
          path,
          table.getRowType(JavaTypeFactoryImpl.INSTANCE).getFieldList(),
          versionContext == null ? null : versionContext.getType().toString(),
          versionContext == null ? null : versionContext.getValue(),
          CatalogUtil.requestedPluginSupportsVersionedTables(path.getRoot(), datasetService.getCatalog()));
      }

      return tool.createPreviewResponseForPhysicalDataset(from, newVersion, fromDatasetPath.toParentPathList(), datasetConfig, references, sql);
    }
  }

  private InitialPreviewResponse newUntitled(FromBase from, DatasetVersion newVersion, List<String> context,
                                             DatasetSummary parentSummary, Integer limit, String engineName,
                                             String sessionId, Map<String, VersionContextReq> references)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return tool.newUntitled(getOrCreateAllocator("newUntitled"),
      from, newVersion, context, parentSummary, false, limit, engineName, sessionId, references);
  }

  /**
   * A user clicked "new query" and then wrote a SQL query. This is the first version of the dataset we will be creating (this is an "initial commit")
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
  @Deprecated
  public InitialPreviewResponse newUntitledSql(
      @QueryParam("newVersion") DatasetVersion newVersion,
      @QueryParam("limit") Integer limit,
      @QueryParam("sessionId") String sessionId,
      /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");
    return newUntitled(
      new FromSQL(
        sql.getSql()).setAlias("nested_0"),
        newVersion,
        sql.getContext(),
        null,
        limit,
        sql.getEngineName(),
        sessionId,
        sql.getReferences());
  }

  /**
   * A user clicked "SQL Runner", then wrote a SQL query and then clicked "Preview". This is the first version of the dataset we will be creating (this is an "initial commit")
   *
   * @param newVersion The version id we should use for the new version of dataset (generated by client)
   * @param sql The sql information to generate the new dataset
   * @param limit The number of records to return in the initial response
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("new_tmp_untitled_sql")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public InitialUntitledRunResponse newTmpUntitledSql(
    @QueryParam("newVersion") DatasetVersion newVersion,
    @QueryParam("limit") Integer limit,
    @QueryParam("sessionId") String sessionId,
    /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");
    return  tool.newTmpUntitled(
      new FromSQL(sql.getSql()).setAlias("nested_0"),
      newVersion,
      sql.getContext(),
      sql.getEngineName(),
      sessionId,
      sql.getReferences());
  }

  @POST @Path("new_untitled_sql_and_run")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public InitialRunResponse newUntitledSqlAndRun(
    @QueryParam("newVersion") DatasetVersion newVersion,
    @QueryParam("sessionId") String sessionId,
      /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");

    return tool.newUntitledAndRun(
      new FromSQL(sql.getSql()).setAlias("nested_0"),
      newVersion,
      sql.getContext(),
      sql.getEngineName(),
      sessionId,
      sql.getReferences());
  }

  @POST @Path("new_tmp_untitled_sql_and_run")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public InitialUntitledRunResponse newTmpUntitledSqlAndRun(
    @QueryParam("newVersion") DatasetVersion newVersion,
    @QueryParam("sessionId") String sessionId,
    /* body */ CreateFromSQL sql)
    throws DatasetNotFoundException, DatasetVersionNotFoundException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");

    return tool.newTmpUntitledAndRun(
      new FromSQL(sql.getSql()).setAlias("nested_0"),
      newVersion,
      sql.getContext(),
      sql.getEngineName(),
      sessionId,
      sql.getReferences());
  }

  /**
   * Create a new query of SELECT * from [parentDataset] which has a POST body.
   *
   * @param parentDataset Parent dataset path
   * @param newVersion The version id we should use for the new version of dataset (generated by client)
   * @param limit The number of records to return to the initial response
   * @param engineName Engine name
   * @param sessionId Session ID
   *
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("new_untitled")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse newUntitledFromParent(
    @QueryParam("parentDataset") DatasetPath parentDataset,
    @QueryParam("newVersion") DatasetVersion newVersion,
    @QueryParam("limit") Integer limit,
    @QueryParam("engineName") String engineName,
    @QueryParam("sessionId") String sessionId,
    @QueryParam("triggerJob") String triggerJob, // "true" or "false". Default - "false". On error - "false"
    /* body */ NewUntitledFromParentRequest reqBody)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    Preconditions.checkNotNull(newVersion, "newVersion should not be null");
    Map<String, VersionContextReq> sourceVersionMap = null;
    if (reqBody != null) {
      sourceVersionMap = reqBody.getReferences();
    }
    try {
      return newUntitled(parentDataset, newVersion, limit, engineName, sessionId, sourceVersionMap, triggerJob);
    } catch (DatasetNotFoundException | NamespaceException e) {
      // TODO: this should really be a separate API from the UI.
      // didn't find as virtual dataset, let's return as opaque sql (as this could be a source) .
      return newUntitled(parentDataset, newVersion, limit, engineName, sessionId, sourceVersionMap, triggerJob);
    }
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
        datasets.add(newDatasetSearchUI(searchEntity.getNamespaceContainer().getDataset(), searchEntity.getCollaborationTag()));
      }
    }
    return datasets;
  }

  protected DatasetSearchUI newDatasetSearchUI(DatasetConfig datasetConfig, CollaborationTag collaborationTag) throws NamespaceException {
    return DatasetSearchUI.newInstance(datasetConfig, collaborationTag);
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
  public DatasetSummary getDatasetSummary(
    @PathParam("path") String path,
    @QueryParam("refType") String refType,
    @QueryParam("refValue") String refValue) throws NamespaceException, DatasetNotFoundException {
    final DatasetPath datasetPath = new DatasetPath(PathUtils.toPathComponents(path));
    return getEnhancedDatasetSummary(datasetPath, DatasetResourceUtils.createSourceVersionMapping(datasetPath.getRoot().getName(), refType, refValue));
  }

  protected User getUser(String username, String entityId) {
    User user = null;
    if (username != null) {
      try {
        user = userService.getUser(username);
      } catch (Exception e) {
        // ignore
      }
    }
    return user;
  }

  private DatasetSummary getEnhancedDatasetSummary(DatasetPath datasetPath,
                                           Map<String, VersionContextReq> references)
    throws NamespaceException, DatasetNotFoundException {
    NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final DremioTable table = getTable(datasetPath, references);
    final DatasetConfig datasetConfig = table.getDatasetConfig();

    String entityId = datasetConfig.getId().getId();
    Optional<Tags> tags = Optional.empty();
    String sourceName = namespaceKey.getRoot();
    boolean isVersioned = CatalogUtil.requestedPluginSupportsVersionedTables(sourceName, datasetService.getCatalog());
    if (!isVersioned) {
      // only use CollaborationHelper for non-versioned dataset from non-versioned source
      // versioned source doesn't rely on NamespaceService while CollaborationHelper use NamespaceService underneath
      tags = collaborationService.getTags(entityId);
    }

    // TODO: DX-61580 Add last modified user to DatasetConfig
    // For now, using the owner as the last modified user. The code is messy. Will be improved in the follow-up story.
    User owner = getUser(datasetConfig.getOwner(), entityId);
    User lastModifyingUser = getUser(datasetConfig.getOwner(), entityId);  // datasetConfig.getLastUser();
    Boolean hasReflection;
    try {
      hasReflection = reflectionServiceHelper.doesDatasetHaveReflection(entityId);
    } catch (AccessControlException e) {
      // If the user doesn't have the proper privilege, set it to null specifically so that it's not even sent back
      hasReflection = null;
    }

    return newDatasetSummary(datasetConfig,
      datasetService.getJobsCount(namespaceKey),
      datasetService.getDescendantsCount(namespaceKey),
      references,
      tags.isPresent() ? tags.get().getTags() : Collections.emptyList(),
      hasReflection, owner, lastModifyingUser);
  }

  protected DatasetSummary newDatasetSummary(
    DatasetConfig datasetConfig,
    int jobCount,
    int descendants,
    Map<String, VersionContextReq> references,
    List<String> tags,
    Boolean hasReflection,
    User owner,
    User lastModifyingUser) throws NamespaceException {
    return DatasetSummary.newInstance(datasetConfig, jobCount, descendants, references, tags, hasReflection, owner, lastModifyingUser);
  }

  protected String getTableDefinition(
    DatasetConfig datasetConfig,
    NamespaceKey resolvedPath,
    List<RelDataTypeField> fields,
    String refType,
    String refValue,
    boolean isVersioned) {
    return new TableDefinitionGenerator(datasetConfig, resolvedPath, fields, refType, refValue, isVersioned, optionManager)
      .generateTableDefinition();
  }
}
