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
import static com.dremio.dac.proto.model.dataset.DataType.FLOAT;
import static com.dremio.dac.proto.model.dataset.DataType.INTEGER;
import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.Transformer.DatasetAndData;
import com.dremio.dac.explore.join.JoinRecommender;
import com.dremio.dac.explore.model.CleanDataCard;
import com.dremio.dac.explore.model.CleanDataCard.ConvertToSingleType;
import com.dremio.dac.explore.model.CleanDataCard.SplitByDataType;
import com.dremio.dac.explore.model.ColumnForCleaning;
import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetResourcePath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.HistogramValue;
import com.dremio.dac.explore.model.History;
import com.dremio.dac.explore.model.HistoryItem;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.explore.model.InitialTransformAndRunResponse;
import com.dremio.dac.explore.model.JoinRecommendations;
import com.dremio.dac.explore.model.ParentDatasetUI;
import com.dremio.dac.explore.model.PreviewReq;
import com.dremio.dac.explore.model.ReplaceValuesPreviewReq;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.explore.model.extract.Card;
import com.dremio.dac.explore.model.extract.Cards;
import com.dremio.dac.explore.model.extract.MapSelection;
import com.dremio.dac.explore.model.extract.ReplaceCards;
import com.dremio.dac.explore.model.extract.ReplaceCards.ReplaceValuesCard;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractMapRule;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.SplitRule;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.resource.BaseResourceWithAllocator;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.ConflictException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.parser.ParserUtil;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsVersionContext;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * All operations related to a given dataset version
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/dataset/{cpath}/version/{version}")
public class DatasetVersionResource extends BaseResourceWithAllocator {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionResource.class);

  private final DatasetTool tool;
  private final QueryExecutor executor;
  private final DatasetVersionMutator datasetService;
  private final Transformer transformer;
  private final Recommenders recommenders;
  private final JoinRecommender joinRecommender;
  private final SecurityContext securityContext;
  private final DatasetPath datasetPath;
  private final DatasetVersion version;
  private final HistogramGenerator histograms;
  @Inject
  public DatasetVersionResource (
    SabotContext context,
    QueryExecutor executor,
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    NamespaceService namespaceService,
    JoinRecommender joinRecommender,
    @Context SecurityContext securityContext,
    @PathParam("cpath") DatasetPath datasetPath,
    @PathParam("version") DatasetVersion version,
    BufferAllocatorFactory allocatorFactory,
    CatalogService catalogService
  ) {
    this(
      executor,
      datasetService,
      new Recommenders(executor, datasetPath, version),
      new Transformer(context, jobsService, namespaceService, datasetService, executor, securityContext, catalogService),
      joinRecommender,
      new DatasetTool(datasetService, jobsService, executor, securityContext),
      new HistogramGenerator(executor),
      securityContext,
      datasetPath,
      version,
      allocatorFactory);
  }

  public DatasetVersionResource(
      QueryExecutor executor,
      DatasetVersionMutator datasetService,
      Recommenders recommenders,
      Transformer transformer,
      JoinRecommender joinRecommender,
      DatasetTool datasetTool,
      HistogramGenerator histograms,
      SecurityContext securityContext,
      DatasetPath datasetPath,
      DatasetVersion version,
      BufferAllocator allocator) {
    super(allocator);
    this.executor = executor;
    this.datasetService = datasetService;
    this.recommenders = recommenders;
    this.transformer = transformer;
    this.joinRecommender = joinRecommender;
    this.tool = datasetTool;
    this.histograms = histograms;
    this.securityContext = securityContext;
    this.datasetPath = datasetPath;
    this.version = version;
  }

  protected DatasetVersionResource(
    QueryExecutor executor,
    DatasetVersionMutator datasetService,
    Recommenders recommenders,
    Transformer transformer,
    JoinRecommender joinRecommender,
    DatasetTool datasetTool,
    HistogramGenerator histograms,
    SecurityContext securityContext,
    DatasetPath datasetPath,
    DatasetVersion version,
    BufferAllocatorFactory allocatorFactory) {
    super(allocatorFactory);
    this.executor = executor;
    this.datasetService = datasetService;
    this.recommenders = recommenders;
    this.transformer = transformer;
    this.joinRecommender = joinRecommender;
    this.tool = datasetTool;
    this.histograms = histograms;
    this.securityContext = securityContext;
    this.datasetPath = datasetPath;
    this.version = version;
  }

  @GET
  @Produces(APPLICATION_JSON)
  public Dataset getDataset() throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    return getCurrentDataset();
  }

  @VisibleForTesting
  protected VirtualDatasetUI getDatasetConfig(boolean isVersionedSource) throws DatasetVersionNotFoundException {
      try {
        return datasetService.getVersion(datasetPath, version, isVersionedSource);
      } catch (DatasetNotFoundException e) {
        try {
          // For history, the UI will request the tip dataset path and the version of the history item, which may
          // actually be referencing another dataset that we derived from.  Therefore, if we fail to find a
          // dataset/version combo, check the history of the tip dataset and search for any entry that matches the
          // specified version.
          VirtualDatasetUI rootDataset = datasetService.get(datasetPath);

          History history = tool.getHistory(datasetPath, rootDataset.getVersion());
          for (HistoryItem historyItem : history.getItems()) {
            if (version.equals(historyItem.getDatasetVersion())) {
              return datasetService.get(historyItem.getDataset(), version);
            }
          }

          throw e;
        } catch (NamespaceException nsException) {
          throw e;
        }
      }
  }

  private Dataset getCurrentDataset() throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    VirtualDatasetUI config = getDatasetConfig(false);
    return Dataset.newInstance(
      new DatasetResourcePath(datasetPath),
      new DatasetVersionResourcePath(datasetPath, version),
      new DatasetName(config.getName()),
      config.getSql(),
      config,
      datasetService.getJobsCount(datasetPath.toNamespaceKey()),
     null
    );
  }

  private DatasetVersionResourcePath resourcePath() {
    return new DatasetVersionResourcePath(datasetPath, version);
  }

  /**
   * Return a preview (sampled) result of the dataset version. Output contains initial data and a pagination URL to
   * fetch remaining data.
   *
   * @return the given version of the dataset
   * @throws DatasetVersionNotFoundException
   */
  @GET @Path("preview")
  @Produces(APPLICATION_JSON)
  public InitialPreviewResponse getDatasetForVersion(
      @QueryParam("tipVersion") DatasetVersion tipVersion,
      @QueryParam("limit") Integer limit,
      @QueryParam("engineName") String engineName,
      @QueryParam("sessionId") String sessionId,
      @QueryParam("refType") String refType,
      @QueryParam("refValue") String refValue,
      @QueryParam("triggerJob") String triggerJob) // "true" or "false". Default - "true". On error - "true"
    throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {
    Catalog catalog = datasetService.getCatalog();
    final boolean versioned = isVersionedPlugin(datasetPath, catalog);

    if (!versioned || tipVersion != null) {
      // tip version is optional, as it is only needed when we are navigated back in history
      // otherwise assume the current version is at the tip of the history
      final VirtualDatasetUI dataset = getDatasetConfig(versioned);

      return tool.createPreviewResponseForExistingDataset(
          dataset,
          new DatasetVersionResourcePath(
              datasetPath, (tipVersion != null) ? tipVersion : dataset.getVersion()),
          engineName,
          sessionId,
          triggerJob);
    }

    // Versioned sources
    // First check if the version already exists as result of running the query
    try {
      final VirtualDatasetUI vds = getDatasetConfig(true);
      return tool.createPreviewResponseForExistingDataset(
        vds,
        new DatasetVersionResourcePath(datasetPath, version),
        engineName,
        sessionId,
        triggerJob);
    } catch (DatasetVersionNotFoundException e) {
      // ignore
    }

    // The version doesn't exist, generate initial preview response from source.
    return getInitialPreviewResponseForVersionedSource(engineName, sessionId, refType, refValue, triggerJob);
  }

  private InitialPreviewResponse getInitialPreviewResponseForVersionedSource(
      String engineName,
      String sessionId,
      String refType,
      String refValue,
      String triggerJob)
    throws NamespaceException, JobNotFoundException {
    if (refType == null || refValue == null) {
      throw UserException
        .validationError()
        .message("Tried to preview a versioned view without a refType/refValue pair")
        .buildSilently();
    }

    final Map<String, VersionContextReq> versionContextReqMapping =
      DatasetResourceUtils.createSourceVersionMapping(datasetPath.getRoot().getName(), refType, refValue);
    final Map<String, VersionContext> versionContextMapping =
      DatasetResourceUtils.createSourceVersionMapping(versionContextReqMapping);

    final Catalog catalog = datasetService.getCatalog().resolveCatalog(versionContextMapping);
    DremioTable table = catalog.getTable(new NamespaceKey(datasetPath.toPathList()));

    if (!(table instanceof ViewTable)) {
      throw UserException.validationError()
        .message("Expecting getting a view but returns a entity type of %s", table.getClass())
        .buildSilently();
    }

    DatasetConfig tableDatasetConfig = table.getDatasetConfig();
    VirtualDataset tableVirtualDataset = tableDatasetConfig.getVirtualDataset();

    VirtualDatasetUI vds = new VirtualDatasetUI();
    vds.setOwner(securityContext.getUserPrincipal().getName());
    vds.setVersion(version);
    vds.setFullPathList(datasetPath.toPathList());
    vds.setName(datasetPath.getDataset().getName());
    vds.setIsNamed(true);
    vds.setId(tableDatasetConfig.getId().getId());
    vds.setContextList(tableVirtualDataset.getContextList());
    vds.setSql(tableVirtualDataset.getSql());
    vds.setSqlFieldsList(tableVirtualDataset.getSqlFieldsList());
    final FromBase from = new FromSQL(tableVirtualDataset.getSql());
    vds.setState(new VirtualDatasetState()
      .setContextList(tableVirtualDataset.getContextList())
      .setFrom(from.wrap()));
    vds.setLastTransform(new Transform(TransformType.createFromParent)
      .setTransformCreateFromParent(new TransformCreateFromParent(from.wrap())));
    final List<SourceVersionReference> sourceVersionReferences =
      DatasetResourceUtils.createSourceVersionReferenceList(versionContextReqMapping);
    vds.setReferencesList(sourceVersionReferences);
    vds.setSavedTag(tableDatasetConfig.getTag());

    logger.debug("Creating temp version {} in datasetVersion for view {} at version {}.",
      DatasetsUtil.printVersionViewInfo(vds),
      datasetPath.toUnescapedString(),
      versionContextMapping.get(datasetPath.getRoot().getName()));
    datasetService.putVersion(vds);
    vds = datasetService.getVersion(datasetPath, version, true);

    return tool.createPreviewResponseForExistingDataset(
      vds,
      new DatasetVersionResourcePath(datasetPath, version),
      engineName,
      sessionId,
      triggerJob);
  }

  @GET @Path("review")
  @Produces(APPLICATION_JSON)
  public InitialPreviewResponse reviewDatasetVersion(
      @QueryParam("jobId") String jobId,
      @QueryParam("tipVersion") DatasetVersion tipVersion,
      @QueryParam("limit") Integer limit)
      throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {
    return tool.createReviewResponse(datasetPath, getDatasetConfig(false), jobId, tipVersion, getOrCreateAllocator("reviewDatasetVersion"), limit);
  }

  /**
   * Apply the given transform on the dataset version and return preview (sampled) results. Also save the
   * transformed dataset as given new version.
   *
   * @param transform
   * @param newVersion
   * @param limit
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("transformAndPreview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialPreviewResponse transformAndPreview(
      /* Body */ TransformBase transform,
      @QueryParam("newVersion") DatasetVersion newVersion,
      @QueryParam("limit") @DefaultValue("50") int limit)
      throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException, JobNotFoundException {
    // TODO - Move transformations to their own Resource file
    if (newVersion == null) {
      throw new ClientErrorException("Query parameter 'newVersion' should not be null");
    }

    final DatasetAndData datasetAndData = transformer.transformWithExecute(newVersion, datasetPath, getDatasetConfig(false), transform, false, QueryType.UI_PREVIEW);
    return tool.createPreviewResponse(datasetPath, datasetAndData, getOrCreateAllocator("transformAndPreview"), limit, false);
  }

  @POST @Path("transform_and_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialPreviewResponse transformAndPreviewAsync(
    /* Body */ TransformBase transform,
               @QueryParam("newVersion") DatasetVersion newVersion,
               @QueryParam("limit") @DefaultValue("50") int limit)
    throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    // TODO - Move transformations to their own Resource file
    if (newVersion == null) {
      throw new ClientErrorException("Query parameter 'newVersion' should not be null");
    }

    final DatasetAndData datasetAndData = transformer.transformWithExecute(newVersion, datasetPath, getDatasetConfig(false), transform, true, QueryType.UI_PREVIEW);

    return InitialPreviewResponse.of(
      newDataset(datasetAndData.getDataset(), null),
      datasetAndData.getJobId(),
      datasetAndData.getSessionId(),
      null,
      true,
      null,
      null); // errors will be retrieved from job status
  }

  /**
   * Apply the given transform on the dataset version and return results. Also save the
   * transformed dataset as given new version.
   *
   * @param transform
   * @param newVersion
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("transformAndRun")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialTransformAndRunResponse transformAndRun(
      /* Body */ TransformBase transform,
      @QueryParam("newVersion") DatasetVersion newVersion
  ) throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    // TODO - Move transformations to their own Resource file
    if (newVersion == null) {
      throw new ClientErrorException("Query parameter 'newVersion' should not be null");
    }

    final DatasetVersionResourcePath resourcePath = resourcePath();
    final DatasetAndData datasetAndData = transformer.transformWithExecute(newVersion, resourcePath.getDataset(), getDatasetConfig(false), transform, false, QueryType.UI_RUN);
    final History history = tool.getHistory(resourcePath.getDataset(), datasetAndData.getDataset().getVersion());
    return InitialTransformAndRunResponse.of(
      newDataset(datasetAndData.getDataset(), null),
      datasetAndData.getJobId(),
      datasetAndData.getSessionId(),
      history);
  }

  /**
   * Apply the given transform on the dataset version and return initial results after the job is started. Also Creating
   * a thread to save the transformed dataset as given new version after the metadata os retrieved.
   *
   * @param transform
   * @param newVersion
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @POST @Path("transform_and_run")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialTransformAndRunResponse transformAndRunAsync(
    /* Body */ TransformBase transform,
               @QueryParam("newVersion") DatasetVersion newVersion
  ) throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    // TODO - Move transformations to their own Resource file
    if (newVersion == null) {
      throw new ClientErrorException("Query parameter 'newVersion' should not be null");
    }

    final DatasetVersionResourcePath resourcePath = resourcePath();
    final DatasetAndData datasetAndData = transformer.transformWithExecute(newVersion, resourcePath.getDataset(), getDatasetConfig(false), transform, true, QueryType.UI_RUN);
    return InitialTransformAndRunResponse.of(
      newDataset(datasetAndData.getDataset(), null),
      datasetAndData.getJobId(),
      datasetAndData.getSessionId(),
      null);
  }

  protected DatasetUI newDataset(VirtualDatasetUI vds, DatasetVersion tipVersion) throws NamespaceException {
    return DatasetUI.newInstance(vds, null, datasetService.getNamespaceService());
  }

  /**
   * Return complete results of a dataset version. Response contains a pagination URL to fetch the data in chunks.
   *
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws InterruptedException
   */
  @GET @Path("run")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialRunResponse run(@QueryParam("tipVersion") DatasetVersion tipVersion,
                                @QueryParam("engineName") String engineName,
                                @QueryParam("sessionId") String sessionId)
    throws DatasetVersionNotFoundException, InterruptedException, NamespaceException {
    final VirtualDatasetUI virtualDatasetUI = getDatasetConfig(false);
    final Map<String, JobsVersionContext> sourceVersionMapping = TransformerUtils.createSourceVersionMapping(virtualDatasetUI.getReferencesList());

    final SqlQuery query = new SqlQuery(virtualDatasetUI.getSql(), virtualDatasetUI.getState().getContextList(), securityContext,
      Strings.isNullOrEmpty(engineName)? null : engineName, sessionId, sourceVersionMapping);
    MetadataJobStatusListener listener = new MetadataJobStatusListener(tool, virtualDatasetUI, null);
    // The saved dataset is incomplete, we want save the dataset again once the metadata is collected.
    if (virtualDatasetUI.getSqlFieldsList() == null) {
      listener.waitToApplyMetadataAndSaveDataset();
    }
    final JobData jobData = executor.runQueryWithListener(query, QueryType.UI_RUN, datasetPath, version, listener);
    final JobId jobId = jobData.getJobId();
    final SessionId jobDataSessionId = jobData.getSessionId();
    if (virtualDatasetUI.getSqlFieldsList() == null) {
      listener.setJobId(jobData.getJobId());
    }

    // tip version is optional, as it is only needed when we are navigated back in history
    // otherwise assume the current version is at the tip of the history
    tipVersion = tipVersion != null ? tipVersion : virtualDatasetUI.getVersion();
    final History history = tool.getHistory(datasetPath, virtualDatasetUI.getVersion(), tipVersion);
    // This is requires as BE generates apiLinks, that is used by UI to send requests for preview/run. In case, when history
    // of a dataset reference on a version for other dataset. And a user navigate to that version and tries to preview it,
    // we would not be resolve a tip version and preview will fail. We should always send requests to original dataset
    // path (tip version path) to be able to get a preview/run data
    // TODO(DX-14701) move links from BE to UI
    virtualDatasetUI.setFullPathList(datasetPath.toPathList());
    return InitialRunResponse.of(newDataset(virtualDatasetUI, tipVersion), jobId, jobDataSessionId, history);
  }


  /**
   * Apply an ephemeral transformation and see the result (note that this does not create a dataset version). Result
   * includes preview (sampled) output, highlighted/deleted columns and marked rows.
   *
   * @param transform
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws NamespaceException
   * @throws DatasetNotFoundException
   */
  @POST @Path("transformPeek")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public InitialPendingTransformResponse transformDataSetPreview(
      /* Body */ TransformBase transform,
      @QueryParam("newVersion") DatasetVersion newVersion,
      @QueryParam("limit") @DefaultValue("50") int limit) throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    // TODO - Move transformations to their own Resource file
    final VirtualDatasetUI virtualDatasetUI = getDatasetConfig(false);
    checkNotNull(virtualDatasetUI.getState());

    return transformer.transformPreviewWithExecute(newVersion, datasetPath, virtualDatasetUI, transform, getOrCreateAllocator("InitialPendingTransformResponse"), limit);
  }

  /**
   * Saves this version as the current version of a dataset under the asDatasetPath if provided
   *
   * @param asDatasetPath
   * @param savedTag the last OCC version known the client. If no one else has saved
   *                     to this name since the client making request learned of this OCC
   *                     version then the request will be successful. Otherwise, it will fail
   *                     because saving would clobber the already saved dataset that the client
   *                     did not know about.
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws NamespaceException
   * @throws DatasetNotFoundException
   */
  @POST @Path("save")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public DatasetUIWithHistory saveAsDataSet(
      @QueryParam("as") DatasetPath asDatasetPath,
      @QueryParam("savedTag") String savedTag, // null for the first save
      @QueryParam("branchName") String branchName
  ) throws DatasetVersionNotFoundException, UserNotFoundException, NamespaceException, DatasetNotFoundException, IOException {
    if (asDatasetPath == null) {
      asDatasetPath = datasetPath;
    }
    // check if source is versioned
    final Catalog catalog = datasetService.getCatalog();
    final boolean versioned = isVersionedPlugin(asDatasetPath, catalog);
    // check if versioned view is enabled
    final boolean versionedViewEnabled = datasetService.checkIfVersionedViewEnabled();
    if(versioned && !versionedViewEnabled){
      throw UserException.unsupportedError().message("Versioned view is not enabled").buildSilently();
    }
    //Gets the latest version of the view from DatasetVersion store
    final VirtualDatasetUI vds = getDatasetConfig(versioned);
    if(vds != null && branchName == null && versioned) {
      branchName = vds
        .getReferencesList()
        .get(0)
        .getReference()
        .getValue();
    }
    if (vds == null) {
      throw new DatasetVersionNotFoundException(datasetPath, version);
    }
    if (versioned) {
      if (branchName != null) {
        setReference(vds, branchName);
      } else {
        throw UserException.unsupportedError().message("Tried to create a versioned view but branch name is null").buildSilently();
      }
    }

    final DatasetUI savedDataset = save(vds, asDatasetPath, savedTag, branchName, versioned);
    return new DatasetUIWithHistory(savedDataset, tool.getHistory(asDatasetPath, savedDataset.getDatasetVersion()));
  }

  protected void setReference(VirtualDatasetUI vds, String reference) {
    List<SourceVersionReference> sourceVersionReferences = vds.getReferencesList();
    SourceVersionReference firstEntry = null;
    if (sourceVersionReferences != null) {
      firstEntry = sourceVersionReferences.get(0);
    }
    if (firstEntry != null) {
      vds.getReferencesList().get(0).getReference().setValue(reference);
    }
  }

  protected boolean isVersionedPlugin(DatasetPath datasetPath, Catalog catalog){
    NamespaceKey namespaceKey = new NamespaceKey(datasetPath.toPathList());
    return CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog);
  }

  private DatasetUI save(VirtualDatasetUI vds, DatasetPath asDatasetPath, String savedTag, NamespaceAttribute... attributes)
      throws DatasetNotFoundException, UserNotFoundException, NamespaceException, DatasetVersionNotFoundException {
    return save(vds, asDatasetPath, savedTag, null, false, attributes);
  }

  public DatasetUI save(VirtualDatasetUI vds, DatasetPath asDatasetPath, String savedTag, String branchName, final boolean isVersionedSource, NamespaceAttribute... attributes)
    throws DatasetNotFoundException, UserNotFoundException, NamespaceException, DatasetVersionNotFoundException {
    checkSaveNonVersionedView(branchName, isVersionedSource);
    String queryString = vds.getSql();
    final boolean isVersionViewEnabled = datasetService.checkIfVersionedViewEnabled();
    ParserUtil.validateViewQuery(queryString);

    if (isVersionedSource) {
      if (ParserUtil.checkTimeTravelOnView(queryString)){
        throw UserException.unsupportedError()
          .message("Versioned views not supported for time travel queries. Please use AT TAG or AT COMMIT instead")
          .buildSilently();
      }
    }

    final List<String> fullPathList = asDatasetPath.toPathList();
    if(!isVersionedSource){
      if (isAncestor(vds, fullPathList)) {
        final String nameConflictErrorMsg = String.format("VDS '%s' already exists. Please enter a different name.",
          asDatasetPath.getLeaf());
        throw new ConflictException(nameConflictErrorMsg);
      }
      if (!datasetPath.equals(asDatasetPath)) {
        // Saving as a new dataset. Reset the Id, so that a new id is created for the new dataset.
        vds.setId(null);
      }
      vds.setSavedTag(savedTag);
    }

    vds.setFullPathList(asDatasetPath.toPathList());
    vds.setName(asDatasetPath.getDataset().getName());
    vds.setIsNamed(true);

    try {
      NameDatasetRef prevDataset = vds.getPreviousVersion();
      NameDatasetRef rewrittenPrev = null;
      if (prevDataset != null) {
        rewrittenPrev = new NameDatasetRef();
        String previousVersion = prevDataset.getDatasetVersion();
        rewrittenPrev.setDatasetVersion(previousVersion);
        if (new DatasetPath(prevDataset.getDatasetPath()).equals(TMP_DATASET_PATH)) {
          rewrittenPrev.setDatasetPath(asDatasetPath.toPathString());
        } else {
          rewrittenPrev.setDatasetPath(prevDataset.getDatasetPath());
        }
        vds.setPreviousVersion(rewrittenPrev);
      }
      if(!isVersionedSource){
        datasetService.put(vds, attributes);
      } else {
        datasetService.putWithVersionedSource(vds, asDatasetPath, branchName, savedTag);
      }

      vds.setPreviousVersion(prevDataset);
      tool.rewriteHistory(vds, asDatasetPath);
      vds.setPreviousVersion(rewrittenPrev);
    } catch(NamespaceNotFoundException nfe) {
      throw new ClientErrorException("Parent folder doesn't exist", nfe);
    } catch(ConcurrentModificationException cme) {
      final String cmeMessage = String.format("View '%s' experienced a concurrent modification exception. Please ensure there are no self-references in your view and no other systems are editing this view.",
        asDatasetPath.getLeaf());
      throw new ConflictException(cmeMessage, cme);
    } catch (IOException e) {
      throw UserException.validationError().message("Error saving to the source: %s", e.getMessage()).buildSilently();
    }

    return newDataset(vds, null);
  }

  private void checkSaveNonVersionedView(String branchName, boolean version){
    if(branchName != null && !version) {
      throw UserException
        .validationError()
        .message("Tried to create a non-versioned view but branch name is not null")
        .buildSilently();
    }
  }

  /**
   * @return true if pathList is an ancestor (parent or grandparent) of the virtual dataset
   */
  private static boolean isAncestor(VirtualDatasetUI vds, List<String> pathList) {
    List<ParentDataset> parents = vds.getParentsList();
    if (parents != null) {
      for (ParentDataset parent : parents) {
        if (pathList.equals(parent.getDatasetPathList())) {
          return true;
        }
      }
    }

    List<ParentDataset> grandParents = vds.getGrandParentsList();
    if (grandParents != null) {
      for (ParentDataset parent : grandParents) {
        if (pathList.equals(parent.getDatasetPathList())) {
          return true;
        }
      }
    }

    return false;
  }

  @POST @Path("extract") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Cards<ExtractRule> getExtractCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    validateColumnType("Extract text", TEXT, selection.getColName());
    List<Card<ExtractRule>> cards = recommenders.recommendExtract(selection, getDatasetSql(), getOrCreateAllocator("getExtractCards"));
    return new Cards<>(cards);
  }

  @POST @Path("extract_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ExtractRule> getExtractCard(
      /* Body */ PreviewReq<ExtractRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    String colName = req.getSelection().getColName();
    return recommenders.generateExtractCard(req.getRule(), colName, getDatasetSql(), getOrCreateAllocator("getExtractCard"));
  }

  @POST @Path("extract_map") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Cards<ExtractMapRule> getExtractMapCards(
      /* Body */ MapSelection mapSelection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    List<Card<ExtractMapRule>> rules = recommenders.recommendExtractMap(mapSelection, getDatasetSql(), getOrCreateAllocator("getExtractMapCards"));
    return new Cards<>(rules);
  }

  @POST @Path("extract_struct_preview") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ExtractMapRule> getExtractMapCard(
      /* Body */ PreviewReq<ExtractMapRule, MapSelection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    String colName = req.getSelection().getColName();
    return recommenders.generateExtractMapCard(req.getRule(), colName, getDatasetSql(), getOrCreateAllocator("getExtractMapCard"));
  }

  @POST @Path("extract_list") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Cards<ExtractListRule> getExtractListCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    List<Card<ExtractListRule>> rules = recommenders.recommendExtractList(selection, getDatasetSql(), getOrCreateAllocator("getExtractListCards"));
    return new Cards<>(rules);
  }

  @POST @Path("extract_list_preview") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ExtractListRule> getExtractListCard(
      /* Body */ PreviewReq<ExtractListRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    String colName = req.getSelection().getColName();
    return recommenders.generateExtractListCard(req.getRule(), colName, getDatasetSql(), getOrCreateAllocator("getExtractListCard"));
  }

  @POST @Path("split") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Cards<SplitRule> getSplitCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    validateColumnType("Split", TEXT, selection.getColName());
    List<Card<SplitRule>> rules = recommenders.recommendSplit(selection, getDatasetSql(), getOrCreateAllocator("getSplitCards"));
    return new Cards<>(rules);
  }

  @POST @Path("split_preview") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<SplitRule> getSplitCard(
      /* Body */ PreviewReq<SplitRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    String colName = req.getSelection().getColName();
    return recommenders.generateSplitCard(req.getRule(), colName, getDatasetSql(), getOrCreateAllocator("getSplitCard"));
  }

  /**
   * reapplies transforms from current dataset to the parent dataset it was created from.
   * @return new version of parent dataset with applied transforms
   * @throws NamespaceException when incorrect datasetPath is provided
   * @throws DatasetNotFoundException if dataset is not found
   * @throws DatasetVersionNotFoundException if dataset version is not found
   */
  @POST @Path("/editOriginalSql")
  @Produces(APPLICATION_JSON)
  public InitialPreviewResponse reapplyDatasetAndPreview() throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException, JobNotFoundException {
    // TODO - Check if this is still used by the UI.
    Transformer.DatasetAndData datasetAndData = reapplyDataset(JobStatusListener.NO_OP);
    //max records = 0 means, that we should not wait for job completion
    return tool.createPreviewResponse(new DatasetPath(datasetAndData.getDataset().getFullPathList()), datasetAndData, getOrCreateAllocator("reapplyDatasetAndPreview"), 0, false);
  }

  private Transformer.DatasetAndData reapplyDataset(JobStatusListener listener) throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    List<VirtualDatasetUI> items = getPreviousDatasetVersions(getDatasetConfig(false));
    List<Transform> transforms = new ArrayList<>();
    for(VirtualDatasetUI dataset : items){
      transforms.add(dataset.getLastTransform());
    }

    return transformer.editOriginalSql(version, transforms, QueryType.UI_PREVIEW, listener);
  }

  @POST @Path("/reapplyAndSave")
  @Produces(APPLICATION_JSON)
  public DatasetUIWithHistory reapplySave(
      @QueryParam("as") DatasetPath asDatasetPath
  ) throws DatasetVersionNotFoundException, UserNotFoundException, DatasetNotFoundException, NamespaceException {
    final CompletionListener completionListener = new CompletionListener();
    Transformer.DatasetAndData datasetAndData = reapplyDataset(completionListener);
    completionListener.awaitUnchecked();
    DatasetUI savedDataset = save(datasetAndData.getDataset(), asDatasetPath, null);
    return new DatasetUIWithHistory(savedDataset, tool.getHistory(asDatasetPath, datasetAndData.getDataset().getVersion()));
  }

  // a partial duplicate of gethistory
  private List<VirtualDatasetUI> getPreviousDatasetVersions(VirtualDatasetUI dataset)
      throws DatasetVersionNotFoundException {
    List<VirtualDatasetUI> items = new ArrayList<>();
    NameDatasetRef previousVersion;
    while (true) {
      items.add(dataset);
      previousVersion = dataset.getPreviousVersion();
      if (previousVersion != null) {
        dataset = datasetService.getVersion(
            new DatasetPath(previousVersion.getDatasetPath()),
            new DatasetVersion(previousVersion.getDatasetVersion()));
      } else {
        break;
      }
    }
    return items;
  }

  @POST @Path("replace") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceCards getReplaceCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getCards(selection);
  }

  private ReplaceValuesCard genReplaceValuesCard(List<String> selected, Selection selection) throws DatasetVersionNotFoundException {
    VirtualDatasetUI virtualDatasetUI = getDatasetConfig(false);
    Set<String> selectedSet = new HashSet<>(selected);
    SqlQuery query = new SqlQuery(virtualDatasetUI.getSql(), virtualDatasetUI.getState().getContextList(), securityContext);
    DataType colType = getColType(selection.getColName());
    HistogramGenerator.Histogram<HistogramValue> histogram = histograms.getHistogram(datasetPath, version, selection, colType, query, getOrCreateAllocator("genReplaceValuesCard"));

    long selectedCount = histograms.getSelectionCount(datasetPath, version, query, colType, selection.getColName(), selectedSet, getOrCreateAllocator("genReplaceValuesCard"));
    return new ReplaceValuesCard(histogram.getValues(), selectedCount, histogram.getAvailableValues() - selectedCount, histogram.getAvailableValues());
  }

  @POST @Path("replace_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ReplacePatternRule> getReplaceCard(
      /* Body */ PreviewReq<ReplacePatternRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getPatternCard(req);
  }

  @POST @Path("replace_values_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceValuesCard getReplaceValuesCard(
      /* Body */ ReplaceValuesPreviewReq req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getValuesCard(req);
  }

  @POST @Path("keeponly") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceCards getKeeponlyCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getCards(selection);
  }

  @POST @Path("keeponly_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ReplacePatternRule> getKeeponlyCard(
      /* Body */ PreviewReq<ReplacePatternRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getPatternCard(req);
  }

  @POST @Path("keeponly_values_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceValuesCard getKeeponlyValuesCard(
      /* Body */ ReplaceValuesPreviewReq req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getValuesCard(req);
  }

  @POST @Path("exclude") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceCards getExcludeCards(
      /* Body */ Selection selection) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getCards(selection);
  }

  @POST @Path("exclude_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public Card<ReplacePatternRule> getExcludeCard(
      /* Body */ PreviewReq<ReplacePatternRule, Selection> req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getPatternCard(req);
  }

  @POST @Path("exclude_values_preview")
  @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public ReplaceValuesCard getExcludeValuesCard(
      /* Body */ ReplaceValuesPreviewReq req) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    return getValuesCard(req);
  }

  private ReplaceCards getCards(Selection selection) throws DatasetVersionNotFoundException {
    final DataType colType = getColType(selection.getColName());
    final List<Card<ReplacePatternRule>> rules;
    if (colType == TEXT) {
      rules = recommenders.recommendReplace(selection, colType, getDatasetSql(), getOrCreateAllocator("getCards"));
    } else {
      // No rules for non-text types
      rules = Collections.emptyList();
    }
    // If the selection is complete cell value, then pass it as selected value to values card generator. This is usually
    // the case of replace/keeponly/exclude selection for non-text types. Better fix is to make the UI send the
    // actual selection (such as number) instead of selection in text format of the cell value.
    List<String> selectedValues;
    if (selection.getCellText() == null || (selection.getOffset() == 0 && selection.getCellText().length() == selection.getLength())) {
      selectedValues = Collections.singletonList(selection.getCellText());
    } else {
      selectedValues = Collections.emptyList();
    }
    ReplaceValuesCard valuesCard = genReplaceValuesCard(selectedValues, selection);
    return new ReplaceCards(rules, valuesCard);
  }

  private Card<ReplacePatternRule> getPatternCard(PreviewReq<ReplacePatternRule, Selection> req)
      throws DatasetVersionNotFoundException {
    String colName = req.getSelection().getColName();
    return recommenders.generateReplaceCard(req.getRule(), colName, getDatasetSql(), getOrCreateAllocator("getPatternCard"));
  }

  private ReplaceValuesCard getValuesCard(ReplaceValuesPreviewReq req) throws DatasetVersionNotFoundException {
    Selection selection = req.getSelection();
    return genReplaceValuesCard(req.getReplacedValues(), selection);
  }

  private static final List<DataType> AVAILABLE_TYPES_FOR_CLEANING = unmodifiableList(asList(TEXT, INTEGER, FLOAT));

  @POST @Path("clean") @Produces(APPLICATION_JSON) @Consumes(APPLICATION_JSON)
  public CleanDataCard getCleanDataCard(
      ColumnForCleaning col) throws DatasetVersionNotFoundException {
    // TODO - Move transformations to their own Resource file
    boolean[] casts = { true, false };
    final VirtualDatasetUI virtualDatasetUI = getDatasetConfig(false);
    String sql = virtualDatasetUI.getSql();
    String colName = col.getColName();
    SqlQuery query = new SqlQuery(sql, virtualDatasetUI.getState().getContextList(), securityContext);
    HistogramGenerator.Histogram<HistogramGenerator.CleanDataHistogramValue> histogram = histograms.getCleanDataHistogram(datasetPath, version, colName, query, getOrCreateAllocator("getCleanDataCard"));
    Map<DataType, Long> typeHistogram = histograms.getTypeHistogram(datasetPath, version, colName, query, getOrCreateAllocator("getCleanDataCard"));
    Set<DataType> foundTypes = new TreeSet<>(typeHistogram.keySet());
    List<SplitByDataType> split = new ArrayList<>();
    List<ConvertToSingleType> convertToSingles = new ArrayList<>();
    List<HistogramValue> values = new ArrayList<>();
    long totalRows = 0;
    for (DataType dataType : foundTypes) {
      totalRows += typeHistogram.get(dataType);
    }
    for (HistogramGenerator.CleanDataHistogramValue histogramValue : histogram.getValues()) {
      values.add(histogramValue.toHistogramValue());
    }
    for (DataType dataTypeForCleaning : AVAILABLE_TYPES_FOR_CLEANING) {
      for (boolean c : casts) {
        long nonMatchingCountForType = 0;
        List<HistogramValue> nonMatchingForType = new ArrayList<>();

        for (HistogramGenerator.CleanDataHistogramValue histogramValue : histogram.getValues()) {
          Boolean isClean = histogramValue.isClean(c, dataTypeForCleaning);
          if (isClean != null && !isClean) {
            nonMatchingCountForType += histogramValue.getCount();
            nonMatchingForType.add(histogramValue.toHistogramValue());
          }
        }
        convertToSingles.add(new ConvertToSingleType(dataTypeForCleaning, c, nonMatchingCountForType, nonMatchingForType));
        // percentage per type for "split per type" pane
      }
      Long count = typeHistogram.get(dataTypeForCleaning);
      double typePercent = count == null ? 0 : (count * 100d / totalRows);
      split.add(new SplitByDataType(dataTypeForCleaning, typePercent));
    }
    return new CleanDataCard(
     // TODO: make sure new col name does not exist
        col.getColName() + "_2", // new name if "clean to single type"
        col.getColName() + "_", // col prefix if "split by data type"
        convertToSingles,
        // info for split pane
        split,
        totalRows, // available values
        values // all values histogram
        );
  }

  @GET
  @Path("join_recs")
  @Produces(APPLICATION_JSON)
  public JoinRecommendations getJoinRecommendations() throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {
    // TODO - Move transformations to their own Resource file
    final Dataset currentDataset = getCurrentDataset();
    return joinRecommender.recommendJoins(currentDataset);
  }

  @GET
  @Path("parents")
  @Produces(APPLICATION_JSON)
  public List<ParentDatasetUI> getParents() throws DatasetNotFoundException, NamespaceException {
    final VirtualDatasetUI virtualDatasetUI = datasetService.get(datasetPath, version);
    final List<ParentDatasetUI> parentDatasetUIs = Lists.newArrayList();
    final List<NamespaceKey> parentDatasetPaths = Lists.newArrayList();
    for (ParentDataset parentDataset : virtualDatasetUI.getParentsList()) {
      parentDatasetPaths.add(new NamespaceKey(parentDataset.getDatasetPathList()));
    }
    for (NameSpaceContainer nameSpaceContainer : datasetService.getNamespaceService().getEntities(parentDatasetPaths)) {
      if (nameSpaceContainer != null && nameSpaceContainer.getType() == Type.DATASET) {
        parentDatasetUIs.add(new ParentDatasetUI(nameSpaceContainer.getFullPathList(), nameSpaceContainer.getDataset().getType()));
      }
    }
    return parentDatasetUIs;
  }

  private DataType getColType(String columnName) throws DatasetVersionNotFoundException {
    for(ViewFieldType type : getDatasetConfig(false).getSqlFieldsList()) {
      if (type.getName().equals(columnName)) {
        return DataTypeUtil.getDataType(SqlTypeName.get(type.getType()));
      }
    }

    throw new ClientErrorException("Given column '" + columnName + "' doesn't exist in dataset");
  }

  private void validateColumnType(String op, DataType expected, String colName) throws DatasetVersionNotFoundException {
    final DataType actual = getColType(colName);
    if (actual != expected) {
      throw new ClientErrorException(String.format("%s is supported only on '%s' type columns. Given type: '%s'",
          op, expected, actual));
    }
  }

  SqlQuery getDatasetSql() throws DatasetVersionNotFoundException {
    final VirtualDatasetUI datasetConfig = getDatasetConfig(false);
    return new SqlQuery(datasetConfig.getSql(), datasetConfig.getState().getContextList(), securityContext.getUserPrincipal().getName());
  }
}
