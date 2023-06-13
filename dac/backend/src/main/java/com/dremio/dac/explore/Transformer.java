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

import static com.dremio.dac.proto.model.dataset.TransformType.join;
import static com.dremio.dac.proto.model.dataset.TransformType.updateSQL;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsVersionContext;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Tool class for applying transformations
 */
public class Transformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Transformer.class);

  public static final String VALUE_PLACEHOLDER = "value";

  private final QueryExecutor executor;
  private final JobsService jobsService;
  private final NamespaceService namespaceService;
  private final DatasetVersionMutator datasetService;
  private final SecurityContext securityContext;
  private final SabotContext context;
  private final CatalogService catalogService;

  public Transformer(SabotContext context, JobsService jobsService, NamespaceService namespace, DatasetVersionMutator datasetService,
                     QueryExecutor executor, SecurityContext securityContext, CatalogService catalogService) {
    this.securityContext = securityContext;
    this.executor = executor;
    this.datasetService = datasetService;
    this.jobsService = jobsService;
    this.namespaceService = namespace;
    this.context = context;
    this.catalogService = catalogService;
  }

  public static String describe(TransformBase transform) {
    if (transform == null) {
      return "New Dataset";
    }
    return transform.accept(new DescribeTransformation());
  }

  private String username(){
    return securityContext.getUserPrincipal().getName();
  }

  /**
   * Protobuf removes nulls inside lists: this makes sure to keep null values as they are
   * by re-adding them after parsing with protobuf. Since replaceNull is not part of protobuf,
   * this is necessary to parse null filter values (most importantly in replaceExact)
   * @param transformResult
   * @param transform
   * @return
   */
  private VirtualDatasetState protectAgainstNull(TransformResult transformResult, TransformBase transform) {
    VirtualDatasetState vss = transformResult.getNewState();
    if (transform instanceof TransformFilter
      && FilterType.Value == ((TransformFilter) transform).getFilter().getType()) {
      List<String> pureFilter = ((TransformFilter) transform).getFilter().getValue().getValuesList();
      vss.getFiltersList()
        .get(vss.getFiltersList().size() - 1)
        .getFilterDef()
        .getValue()
        .setValuesList(pureFilter);
    }
    return vss;
  }

  /**
   * Reaply the provided operations onto the original dataset and execute it.
   * @param newVersion
   * @param operations
   * @return
   * @throws NamespaceException
   * @throws DatasetNotFoundException
   * @throws DatasetVersionNotFoundException
   */
  //TODO (DX-18918: Transformer.editOriginalSql() shouldn't require a JobStatusListener)
  public DatasetAndData editOriginalSql(DatasetVersion newVersion, List<Transform> operations, QueryType queryType,
    JobStatusListener listener) throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {

    // apply transformations bottom up.
    Collections.reverse(operations);

    final Transform firstVersion = operations.get(0);
    if (firstVersion.getType() != TransformType.createFromParent) {
      throw UserException.unsupportedError()
        .message("Cannot be reapplied because it was not created from a parent dataset")
        .build(logger);
    }
    final TransformCreateFromParent firstTransform = firstVersion.getTransformCreateFromParent();
    if (firstTransform.getCreateFrom().getType() != FromType.Table) {
      throw UserException.unsupportedError()
        .message("Cannot be reapplied because it was not created from a table")
        .build(logger);
    }

    final DatasetPath headPath = new DatasetPath(firstTransform.getCreateFrom().getTable().getDatasetPath());
    final DatasetConfig headConfig = namespaceService.getDataset(headPath.toNamespaceKey());
    if(headConfig == null) {
      throw UserException.unsupportedError()
        .message("The original dataset you are attempting to edit is no longer available.")
        .addContext("Original dataset", headPath.toParentPath())
        .build(logger);

    }

    //TODO: This should verify the save version matches
    VirtualDatasetUI headVersion = datasetService.getVersion(headPath, headConfig.getVirtualDataset().getVersion());

    JobData jobData = executor.runQueryWithListener(new SqlQuery(headVersion.getSql(),
        headVersion.getState().getContextList(), username()), queryType, headPath, headVersion.getVersion(), listener);
    return new DatasetAndData(jobData, headVersion);
  }

  /**
   * Apply a transformation without executing a query. This does partial query parsing & planning so that we can acquire the information necessary.
   * @param newVersion
   * @param path
   * @param baseDataset
   * @param transform
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  public VirtualDatasetUI transformWithExtract(DatasetVersion newVersion, DatasetPath path, VirtualDatasetUI baseDataset, TransformBase transform) throws DatasetNotFoundException, NamespaceException{
    final ExtractTransformActor actor = new ExtractTransformActor(baseDataset.getState(), false, username(), executor);
    final TransformResult result = transform.accept(actor);
    if (!actor.hasMetadata()) {
      VirtualDatasetState vss = protectAgainstNull(result, transform);
      actor.getMetadata(new SqlQuery(SQLGenerator.generateSQL(vss), vss.getContextList(), securityContext));
    }
    VirtualDatasetUI dataset = asDataset(newVersion, path, baseDataset, transform, result, actor, catalogService);

    // save the dataset version.
    datasetService.putVersion(dataset);

    return dataset;
  }

  /**
   * Convert a transform result into a dataset.
   * @param newVersion The new version for the dataset.
   * @param path The path of the dataset.
   * @param baseDataset The original dataset. It's path could differs from {@code path} parameter. (Start edit a dataset
   *                    under {@code path} path. Go back in history to aversion, that corresponds to a {@code baseDataset}
   *                   in other path. Alter a query and preview.
   * @param transform The transformation that was applied
   * @param result The result of the transformation.
   * @param actor The result with metadata information
   * @param catalogService The catalog service needs to incorporate in SQL Generator
   * @return The new VirtualDatasetUI object.
   */
  private VirtualDatasetUI asDataset(
      DatasetVersion newVersion,
      DatasetPath path,
      VirtualDatasetUI baseDataset,
      TransformBase transform,
      TransformResult result,
      TransformActor actor,
      CatalogService catalogService) {
    // here we should take a path from baseDataset, as path could be different
    baseDataset.setPreviousVersion(new NameDatasetRef(DatasetPath.defaultImpl(baseDataset.getFullPathList()).toString())
      .setDatasetVersion(baseDataset.getVersion().toString()));

    baseDataset.setVersion(newVersion);
    // update a path with actual path
    baseDataset.setFullPathList(path.toPathList());
    baseDataset.setState(protectAgainstNull(result, transform));
    // If the user edited the SQL manually we want to keep the SQL as is
    // SQLGenerator.generateSQL(result.getNewState()) is functionally equivalent but not exactly the same
    String sql =
        transform.wrap().getType() == updateSQL ?
        ((TransformUpdateSQL)transform).getSql() :
        SQLGenerator.generateSQL(protectAgainstNull(result, transform), isSupportedTransform(transform), catalogService);
    baseDataset.setSql(sql);
    baseDataset.setLastTransform(transform.wrap());
    if (actor != null && actor.hasMetadata()) {
      DatasetTool.applyQueryMetadata(baseDataset, actor.getParents(), actor.getBatchSchema(), actor.getFieldOrigins(),
        actor.getGrandParents(), actor.getMetadata());
    }
    return baseDataset;
  }

  /**
   * Apply a transformation and start a job. Any transformation operations that
   * require parsing/planning will be done as part of the job. This should
   * typically be the method used for operations.
   *
   * @param newVersion
   * @param path
   * @param original
   * @param transform
   * @param queryType
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  public DatasetAndData transformWithExecute(
      DatasetVersion newVersion,
      DatasetPath path,
      VirtualDatasetUI original,
      TransformBase transform,
      boolean isAsync,
      QueryType queryType)
          throws DatasetNotFoundException {
    return this.transformWithExecute(newVersion, path, original, transform, isAsync, queryType, false);
  }

  /**
   * Applying a transform preview and start the job.
   *
   * @param newVersion
   * @param path
   * @param original
   * @param transform
   * @param limit Max number of rows to return in initial response preview data.
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  public InitialPendingTransformResponse transformPreviewWithExecute(
      DatasetVersion newVersion,
      DatasetPath path,
      VirtualDatasetUI original,
      TransformBase transform,
      BufferAllocator allocator,
      int limit)
      throws DatasetNotFoundException, NamespaceException {
    final TransformResultDatasetAndData result = this.transformWithExecute(newVersion, path, original, transform, false, QueryType.UI_PREVIEW, true);
    final TransformResult transformResult = result.getTransformResult();
    final List<String> highlightedColumnNames = Lists.newArrayList(transformResult.getModifiedColumns());
    highlightedColumnNames.addAll(transformResult.getAddedColumns());

    JobDataClientUtils.waitForFinalState(jobsService, result.getJobId());
    return InitialPendingTransformResponse.of(
        result.getDataset().getSql(),
        result.getJobData().truncate(allocator, limit),
        highlightedColumnNames,
        Lists.newArrayList(transformResult.getRemovedColumns()),
        transformResult.getRowDeletionMarkerColumns()
    );
  }

  /**
   * DatasetAndData
   */
  public static class DatasetAndData {
    private final JobData jobData;
    private final VirtualDatasetUI dataset;

    public DatasetAndData(JobData jobData, VirtualDatasetUI dataset) {
      super();
      this.jobData = jobData;
      this.dataset = dataset;
    }

    public JobId getJobId() {
      return jobData.getJobId();
    }

    public SessionId getSessionId() {
      return jobData.getSessionId();
    }

    public JobData getJobData() {
      return jobData;
    }

    public VirtualDatasetUI getDataset() {
      return dataset;
    }

  }

  private TransformResultDatasetAndData transformWithExecute(
    DatasetVersion newVersion,
    DatasetPath path,
    VirtualDatasetUI original,
    TransformBase transform,
    boolean isAsync,
    QueryType queryType,
    boolean isPreview)
    throws DatasetNotFoundException {

    if (isAsync && transform.wrap().getType() == updateSQL) {
      return updateSQLTransformWithExecuteAsync(newVersion, path, original, transform, queryType, isPreview);
    }

    final ExecuteTransformActor actor = new ExecuteTransformActor(queryType, newVersion, original.getState(), isPreview, username(), path, executor);
    final TransformResult transformResult = transform.accept(actor);
    setReferencesInVirtualDatasetUI(original, transform);
    if (!actor.hasMetadata()) {
      VirtualDatasetState vss = protectAgainstNull(transformResult, transform);
      Map<String, JobsVersionContext> sourceVersionMapping = TransformerUtils.createSourceVersionMapping(transform.getReferencesList());
      String sql = SQLGenerator.generateSQL(vss, isSupportedTransform(transform), catalogService);
      final SqlQuery query = new SqlQuery(sql, vss.getContextList(), securityContext, sourceVersionMapping);
      actor.getMetadata(query);
    }
    final TransformResultDatasetAndData resultToReturn = new TransformResultDatasetAndData(actor.getJobData(),
      asDataset(newVersion, path, original, transform, transformResult, actor, catalogService), transformResult);
    // save this dataset version.
    datasetService.putVersion(resultToReturn.getDataset());

    return resultToReturn;
  }

  // If isAsync is true and the transform type is updateSQL, we need skip the actor visit and submit the query later
  // asynchronously.  The reason is that actor executes the query in visit() when the transform type is updateSQL.
  private TransformResultDatasetAndData updateSQLTransformWithExecuteAsync(
    DatasetVersion newVersion,
    DatasetPath path,
    VirtualDatasetUI original,
    TransformBase transform,
    QueryType queryType,
    boolean isPreview)
    throws DatasetNotFoundException {

    Preconditions.checkArgument(transform.wrap().getType() == updateSQL);
    final ExecuteTransformActor actor = new ExecuteTransformActor(queryType, newVersion, original.getState(), isPreview, username(), path, executor);
    final TransformResult transformResult = new TransformResult(
      new VirtualDatasetState()
        .setFrom(new From(FromType.SQL).setSql(new FromSQL(transform.wrap().getUpdateSQL().getSql())))
        .setContextList(transform.wrap().getUpdateSQL().getSqlContextList()));
    setReferencesInVirtualDatasetUI(original, transform);
    VirtualDatasetUI dataset = asDataset(newVersion, path, original, transform, transformResult, actor, catalogService);

    VirtualDatasetState vss = protectAgainstNull(transformResult, transform);
    Map<String, JobsVersionContext> sourceVersionMapping = TransformerUtils.createSourceVersionMapping(transform.getReferencesList());
    String sql = SQLGenerator.generateSQL(vss, isSupportedTransform(transform), catalogService);

    SqlQuery query = new SqlQuery(sql, vss.getContextList(), securityContext, transform.wrap().getUpdateSQL().getEngineName(),
      transform.wrap().getUpdateSQL().getSessionId(), sourceVersionMapping);
    AsyncMetadataJobStatusListener.MetaDataListener listener = new AsyncMetadataJobStatusListener.MetaDataListener() {
      @Override
      public void metadataCollected(com.dremio.service.jobs.metadata.proto.QueryMetadata metadata) {
        // save this dataset version.
        if (actor.hasMetadata()) {
          DatasetTool.applyQueryMetadata(dataset, actor.getParents(), actor.getBatchSchema(), actor.getFieldOrigins(),
            actor.getGrandParents(), actor.getMetadata());
          dataset.setState(QuerySemantics.extract(actor.getMetadata()));
        }
        datasetService.putVersion(dataset);
      }
    };
    actor.getMetadataAsync(query, listener);

    final TransformResultDatasetAndData resultToReturn = new TransformResultDatasetAndData(actor.getJobData(),
      dataset, transformResult);

    return resultToReturn;
  }

  public boolean isSupportedTransform(TransformBase transform) {
    return transform.wrap().getType() != updateSQL;
  }

  /**
   * Set the references in VirtualDatasetUI from the tranform api calls
   *
   * @param original
   * @param transform
   */
  private void setReferencesInVirtualDatasetUI(
    VirtualDatasetUI original,
    TransformBase transform) {
    List<SourceVersionReference> sourceVersionReferenceList = transform.getReferencesList();

    // No need to change reference in case of Join as it might change the original reference where original table is coming from;
    // Don't change the reference to null as it can be set null by an API call which is not supporting/ setting in-correctly
    if (!(transform.wrap().getType() == join) && sourceVersionReferenceList != null) {
      original.setReferencesList(sourceVersionReferenceList);
    }
  }

  private static class TransformResultDatasetAndData extends DatasetAndData {
    private final TransformResult transformResult;

    public TransformResultDatasetAndData(JobData jobData, VirtualDatasetUI dataset, TransformResult transformResult) {
      super(jobData, dataset);
      this.transformResult = transformResult;
    }

    public TransformResult getTransformResult() {
      return transformResult;
    }
  }

  private class ExtractTransformActor extends TransformActor {

    private volatile QueryMetadata metadata;

    public ExtractTransformActor(
        VirtualDatasetState initialState,
        boolean preview,
        String username,
        QueryExecutor executor) {
      super(initialState, preview, username, executor);
    }

    @Override
    protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata(SqlQuery query) {
      this.metadata = QueryParser.extract(query, context);
      return JobsProtoUtil.toBuf(metadata);
    }

    @Override
    protected boolean hasMetadata() {
      return (metadata != null);
    }

    @Override
    public com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata() {
      Preconditions.checkNotNull(metadata);
      return JobsProtoUtil.toBuf(metadata);
    }

    @Override
    protected Optional<BatchSchema> getBatchSchema() {
      return metadata.getBatchSchema();
    }

    @Override
    protected Optional<List<ParentDatasetInfo>> getParents() {
      return metadata.getParents();
    }

    @Override
    protected Optional<List<FieldOrigin>> getFieldOrigins() {
      return metadata.getFieldOrigins();
    }

    @Override
    protected Optional<List<ParentDataset>> getGrandParents() {
      return metadata.getGrandParents();
    }
  }

  @VisibleForTesting
  class ExecuteTransformActor extends TransformActor {

    private final MetadataCollectingJobStatusListener collector = new MetadataCollectingJobStatusListener();
    private volatile com.dremio.service.jobs.metadata.proto.QueryMetadata metadata;
    private volatile Optional<BatchSchema> batchSchema;
    private volatile Optional<List<ParentDatasetInfo>> parents;
    private volatile Optional<List<FieldOrigin>> fieldOrigins;
    private volatile Optional<List<ParentDataset>> grandParents;
    private volatile JobData jobData;
    private final DatasetPath path;
    private final DatasetVersion newVersion;
    private final QueryType queryType;

    public ExecuteTransformActor(
        QueryType queryType,
        DatasetVersion newVersion,
        VirtualDatasetState initialState,
        boolean preview,
        String username,
        DatasetPath path,
        QueryExecutor executor) {
      super(initialState, preview, username, executor);
      this.path = path;
      this.newVersion = newVersion;
      this.queryType = queryType;
    }

    private void applyMetadata(com.dremio.service.jobs.metadata.proto.QueryMetadata metadata, SqlQuery query) {
      JobId jobId = null;
      SessionId sessionId = null;
      try {
        jobId = jobData.getJobId();
        sessionId = jobData.getSessionId();
        this.metadata = metadata;
        final JobDetails jobDetails = jobsService.getJobDetails(
          JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setUserName(username())
            .build());
        final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();
        this.batchSchema = Optional.ofNullable(jobInfo.getBatchSchema()).map((b) -> BatchSchema.deserialize(b));
        this.parents = Optional.ofNullable(jobInfo.getParentsList());
        this.fieldOrigins = Optional.ofNullable(jobInfo.getFieldOriginsList());
        this.grandParents = Optional.ofNullable(jobInfo.getGrandParentsList());
      } catch (UserException e) {
        // If the original query fails, let the user knows about
        throw DatasetTool.toInvalidQueryException(e, query.getSql(), query.getContext(), null, jobId, sessionId);
      } catch (JobNotFoundException e) {
        UserException uex = UserException.schemaChangeError(e).buildSilently();
        throw DatasetTool.toInvalidQueryException(uex, query.getSql(), query.getContext(), null, jobId, sessionId);
      }

      // If above QueryExecutor finds the query in the job store, QueryMetadata will never be set.
      // In this case, regenerate QueryMetadata below.
      if (this.metadata == null) {
        final QueryMetadata queryMetadata = QueryParser.extract(query, context);
        this.metadata = JobsProtoUtil.toBuf(queryMetadata);
        this.batchSchema = queryMetadata.getBatchSchema();
        this.parents = queryMetadata.getParents();
      }
    }

    @Override
    protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata(SqlQuery query) {
      this.jobData = executor.runQueryWithListener(query, queryType, path, newVersion, collector);
      applyMetadata(collector.getMetadata(), query);

      return metadata;
    }

    protected void getMetadataAsync(SqlQuery query, AsyncMetadataJobStatusListener.MetaDataListener listener) {
      AsyncMetadataJobStatusListener.MetaDataListener metadataListener = new AsyncMetadataJobStatusListener.MetaDataListener() {
        @Override
        public void metadataCollected(com.dremio.service.jobs.metadata.proto.QueryMetadata metadata) {
          ExecuteTransformActor.this.applyMetadata(metadata, query);
        }
      };
      AsyncMetadataJobStatusListener asyncListener = new AsyncMetadataJobStatusListener(metadataListener);
      asyncListener.addMetadataListener(listener);

      this.jobData = executor.runQueryWithListener(query, queryType, path, newVersion, asyncListener);
    }

    @Override
    protected boolean hasMetadata() {
      return (metadata != null);
    }

    @Override
    public com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata() {
      Preconditions.checkNotNull(metadata);
      return metadata;
    }

    @Override
    protected Optional<BatchSchema> getBatchSchema() {
      return batchSchema;
    }

    @Override
    protected Optional<List<ParentDatasetInfo>> getParents() {
      return parents;
    }

    @Override
    protected Optional<List<FieldOrigin>> getFieldOrigins() {
      return fieldOrigins;
    }

    @Override
    protected Optional<List<ParentDataset>> getGrandParents() {
      return grandParents;
    }

    public JobData getJobData() {
      return jobData;
    }

  }


}
