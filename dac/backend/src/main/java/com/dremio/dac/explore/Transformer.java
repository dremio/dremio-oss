/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.dac.proto.model.dataset.TransformType.updateSQL;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
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
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Tool class for applying transformations
 */
class Transformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Transformer.class);

  public static final String VALUE_PLACEHOLDER = "value";

  private final QueryExecutor executor;
  private final NamespaceService namespaceService;
  private final DatasetVersionMutator datasetService;
  private final SecurityContext securityContext;
  private final SabotContext context;

  public Transformer(SabotContext context, NamespaceService namespace, DatasetVersionMutator datasetService, QueryExecutor executor, SecurityContext securityContext) {
    this.securityContext = securityContext;
    this.executor = executor;
    this.datasetService = datasetService;
    this.namespaceService = namespace;
    this.context = context;
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
  public DatasetAndJob reapply(DatasetVersion newVersion, List<Transform> operations, QueryType queryType) throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {

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
        .message("The original dataset you are attempting to reapply your changes onto is no longer available.")
        .addContext("Original dataset", headPath.toParentPath())
        .build(logger);

    }

    //TODO: This should verify the save version matches
    VirtualDatasetUI headVersion = datasetService.getVersion(headPath, headConfig.getVirtualDataset().getVersion());

    // if there are no operations after create, just return existing dataset.
    if(operations.size() == 1) {
      JobUI job = executor.runQuery(new SqlQuery(headVersion.getSql(), headVersion.getState().getContextList(), username()), queryType, headPath, headVersion.getVersion());
      return new DatasetAndJob(job, headVersion);
    }

    // if the transform is a single operation (plus the create from), we just need to apply and execute.
    if(operations.size() == 2){
      return transformWithExecute(newVersion, headPath, headVersion, TransformBase.unwrap(operations.get(1)), queryType);
    }

    // otherwise, apply second through second to last. Then do execution on last.
    final List<Transform> transformVersions = operations.subList(1, operations.size()-1);
    final Transform finalTransform = operations.get(operations.size()-1);

    VirtualDatasetUI previousDataset = headVersion;

    // loop zero or more times to apply intermediate transformations.
    for (Transform transform : transformVersions) {
      previousDataset = transformWithExtract(DatasetVersion.newVersion(), headPath, previousDataset, TransformBase.unwrap(transform));
    }

    return transformWithExecute(newVersion, headPath, previousDataset, TransformBase.unwrap(finalTransform), queryType);
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
  @VisibleForTesting
  VirtualDatasetUI transformWithExtract(DatasetVersion newVersion, DatasetPath path, VirtualDatasetUI baseDataset, TransformBase transform) throws DatasetNotFoundException, NamespaceException{
    final ExtractTransformActor actor = new ExtractTransformActor(baseDataset.getState(), false, username(), executor);
    final TransformResult result = transform.accept(actor);
    if (!actor.hasMetadata()) {
      VirtualDatasetState vss = protectAgainstNull(result, transform);
      actor.getMetadata(new SqlQuery(SQLGenerator.generateSQL(vss), vss.getContextList(), securityContext));
    }
    VirtualDatasetUI dataset = asDataset(newVersion, path, baseDataset, transform, result, actor.getMetadata());

    // save the dataset version.
    datasetService.putVersion(dataset);

    return dataset;
  }

  /**
   * Convert a transform result into a dataset.
   * @param newVersion The new version for the dataset.
   * @param path The path of the dataset.
   * @param baseDataset The original dataset.
   * @param transform The transformation that was applied
   * @param result The result of the transformation.
   * @param metadata The query metadata associated with the transformation.
   * @return The new VirtualDatasetUI object.
   */
  private VirtualDatasetUI asDataset(DatasetVersion newVersion, DatasetPath path, VirtualDatasetUI baseDataset, TransformBase transform, TransformResult result, QueryMetadata metadata) {
    baseDataset.setPreviousVersion(new NameDatasetRef(path.toString()).setDatasetVersion(baseDataset.getVersion().toString()));
    baseDataset.setVersion(newVersion);
    baseDataset.setState(protectAgainstNull(result, transform));
    // If the user edited the SQL manually we want to keep the SQL as is
    // SQLGenerator.generateSQL(result.getNewState()) is functionally equivalent but not exactly the same
    String sql =
        transform.wrap().getType() == updateSQL ?
        ((TransformUpdateSQL)transform).getSql() :
        SQLGenerator.generateSQL(protectAgainstNull(result, transform));
    baseDataset.setSql(sql);
    baseDataset.setLastTransform(transform.wrap());
    DatasetTool.applyQueryMetadata(baseDataset, metadata);
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
  public DatasetAndJob transformWithExecute(
      DatasetVersion newVersion,
      DatasetPath path,
      VirtualDatasetUI original,
      TransformBase transform,
      QueryType queryType)
          throws DatasetNotFoundException, NamespaceException {
    final ExecuteTransformActor actor = new ExecuteTransformActor(queryType, newVersion, original.getState(), false, username(), path, executor);
    final TransformResult transformResult = transform.accept(actor);

    if (!actor.hasMetadata()) {
      VirtualDatasetState vss = protectAgainstNull(transformResult, transform);
      final SqlQuery query = new SqlQuery(SQLGenerator.generateSQL(vss), vss.getContextList(), securityContext);
      actor.getMetadata(query);
    }
    final DatasetAndJob resultToReturn = new DatasetAndJob(actor.getJob(), asDataset(newVersion, path, original,
      transform, transformResult, actor.getMetadata()));
    // save this dataset version.
    datasetService.putVersion(resultToReturn.getDataset());

    return resultToReturn;
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
      int limit)
      throws DatasetNotFoundException, NamespaceException {
    final ExecuteTransformActor actor = new ExecuteTransformActor(QueryType.UI_PREVIEW, newVersion, original.getState(), true, username(), path, executor);
    final TransformResult transformResult = transform.accept(actor);

    if (!actor.hasMetadata()) {
      VirtualDatasetState vss = protectAgainstNull(transformResult, transform);
      final SqlQuery query = new SqlQuery(SQLGenerator.generateSQL(vss), vss.getContextList(), securityContext);
      actor.getMetadata(query);
    }
    final JobUI job = actor.getJob();
    final VirtualDatasetUI dataset = asDataset(newVersion, path, original, transform, transformResult, actor.getMetadata());

    // save this dataset version.
    datasetService.putVersion(dataset);

    final List<String> highlightedColumnNames = Lists.newArrayList(transformResult.getModifiedColumns());
    highlightedColumnNames.addAll(transformResult.getAddedColumns());

    return InitialPendingTransformResponse.of(
        dataset.getSql(),
        job.getData().truncate(limit),
        highlightedColumnNames,
        Lists.newArrayList(transformResult.getRemovedColumns()),
        transformResult.getRowDeletionMarkerColumns()
    );
  }

  public static class DatasetAndJob {
    private final JobUI job;
    private final VirtualDatasetUI dataset;

    public DatasetAndJob(JobUI job, VirtualDatasetUI dataset) {
      super();
      this.job = job;
      this.dataset = dataset;
    }

    public JobUI getJob() {
      return job;
    }

    public VirtualDatasetUI getDataset() {
      return dataset;
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
    protected QueryMetadata getMetadata(SqlQuery query) {
      this.metadata = QueryParser.extract(query, context);
      return metadata;
    }

    @Override
    protected boolean hasMetadata() {
      return (metadata != null);
    }

    public QueryMetadata getMetadata() {
      Preconditions.checkNotNull(metadata);
      return metadata;
    }

  }

  @VisibleForTesting
  class ExecuteTransformActor extends TransformActor {

    private final MetadataCollectingJobStatusListener collector = new MetadataCollectingJobStatusListener();
    private volatile QueryMetadata metadata;
    private volatile JobUI job;
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

    @Override
    protected QueryMetadata getMetadata(SqlQuery query) {
      this.job = executor.runQueryWithListener(query, queryType, path, newVersion, collector);
      this.metadata = collector.getMetadata();

      // If above QueryExecutor finds the query in the job store, QueryMetadata will never be set.
      // In this case, regenerate QueryMetadata below.
      if (this.metadata == null) {
        this.metadata = QueryParser.extract(query, context);
      }

      return metadata;
    }

    @Override
    protected boolean hasMetadata() {
      return (metadata != null);
    }

    public QueryMetadata getMetadata() {
      Preconditions.checkNotNull(metadata);
      return metadata;
    }

    public JobUI getJob() {
      return job;
    }

  }


}
