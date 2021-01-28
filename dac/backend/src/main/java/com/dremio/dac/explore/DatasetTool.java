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

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.dac.explore.model.InitialPreviewResponse.INITIAL_RESULTSET_SIZE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.Column;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.History;
import com.dremio.dac.explore.model.HistoryItem;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.job.QueryError;
import com.dremio.dac.model.spaces.TempSpace;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Derivation;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.server.ApiErrorModel;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.errors.InvalidQueryException;
import com.dremio.dac.service.errors.NewDatasetQueryException;
import com.dremio.dac.util.InvalidQueryErrorConverter;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

/**
 * Class that helps with generating common dataset patterns.
 */
public class DatasetTool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetTool.class);

  private final DatasetVersionMutator datasetService;
  private final JobsService jobsService;
  private final QueryExecutor executor;
  private final SecurityContext context;
  public static final DatasetPath TMP_DATASET_PATH = new DatasetPath(TempSpace.impl(), new DatasetName("UNTITLED"));

  public DatasetTool(
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    QueryExecutor executor,
    SecurityContext context) {
    this.datasetService = datasetService;
    this.jobsService = jobsService;
    this.executor = executor;
    this.context = context;
  }

  /**
   * Helper method to create {@link InitialPreviewResponse} for existing dataset.
   * @param newDataset
   * @param tipVersion - a top level history version for a dataset.
   * @return
   * @throws DatasetVersionNotFoundException
   */
  InitialPreviewResponse createPreviewResponseForExistingDataset (
      BufferAllocator allocator,
      VirtualDatasetUI newDataset,
      DatasetVersionResourcePath tipVersion,
      Integer limit,
      String engineName
      ) throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {

    SqlQuery query = new SqlQuery(newDataset.getSql(), newDataset.getState().getContextList(), username(), engineName);
    JobData jobData = executor.runQueryWithListener(query, QueryType.UI_PREVIEW, tipVersion.getDataset(), newDataset.getVersion(), JobStatusListener.NO_OP);

    return createPreviewResponse(newDataset, jobData, tipVersion, allocator, limit, true);
  }

  private String username(){
    return context.getUserPrincipal().getName();
  }

  /**
   * Helper method to create {@link InitialPreviewResponse} from given inputs
   * @param datasetUI
   * @param job
   * @param tipVersion a combination of dataset verion + path to a dataset. It represent a top history version. Path here
   *                   could differs from path that {@code datasetUI} has, as {@code datasetUI} could be
   *                   a history version, that references on other dataset with different path.
   * @param maxRecords
   * @param catchExecutionError
   * @return
   * @throws DatasetVersionNotFoundException
   * @throws NamespaceException
   * @throws JobNotFoundException
   */
  InitialPreviewResponse createPreviewResponse(VirtualDatasetUI datasetUI, JobData job, DatasetVersionResourcePath tipVersion, BufferAllocator allocator,
      Integer maxRecords, boolean catchExecutionError) throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {
    JobDataFragment dataLimited = null;
    ApiErrorModel<?> error = null;
    try (AutoCloseables.RollbackCloseable cls = new AutoCloseables.RollbackCloseable(true)) {
      if (maxRecords == null) {
        maxRecords = INITIAL_RESULTSET_SIZE;
      }

      try {
        if (maxRecords > 0) {
          JobDataClientUtils.waitForFinalState(jobsService, job.getJobId());
          dataLimited = cls.add(job.truncate(allocator, maxRecords));
        } else {
          final JobDetailsRequest request = JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
            .setUserName(username())
            .build();
          JobDetails jobDetails = jobsService.getJobDetails(request);
          if (JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getBatchSchema() == null) {
            JobDataClientUtils.waitForBatchSchema(jobsService, JobsProtoUtil.toStuff(jobDetails.getJobId()));
            jobDetails = jobsService.getJobDetails(request);
          }
          dataLimited = cls.add(getDataOnlyWithColumns(
            JobsProtoUtil.toStuff(jobDetails.getJobId()),
            JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getBatchSchema()));
        }
      } catch (Exception ex) {
        if (!catchExecutionError) {
          throw ex;
        }

        if (ex instanceof UserException) {
          toInvalidQueryException((UserException) ex, datasetUI.getSql(), ImmutableList.<String>of());
        }
        error = new ApiErrorModel<Void>(ApiErrorModel.ErrorType.INITIAL_PREVIEW_ERROR, ex.getMessage(), GenericErrorMessage.printStackTrace(ex), null);
      }
      cls.commit();
    } catch (DatasetVersionNotFoundException | NamespaceException | JobNotFoundException ex) {
      throw ex;
    } catch (Exception ex) {
      Throwables.throwIfUnchecked(ex);
      throw new RuntimeException(ex);
    }
    final History history = getHistory(tipVersion.getDataset(), datasetUI.getVersion(), tipVersion.getVersion());
    // VBesschetnov 2019-01-08
    // this is requires as BE generates apiLinks, that is used by UI to send requests for preview/run. In case, when history
    // of a dataset reference on a version for other dataset. And a user navigate to that version and tries to preview it,
    // we would not be resolve a tip version and preview will fail. We should always send requests to original dataset
    // path (tip version path) to be able to get a preview/run data
    // TODO(DX-14701) move links from BE to UI
    datasetUI.setFullPathList(tipVersion.getDataset().toPathList());
    return InitialPreviewResponse.of(newDataset(datasetUI, tipVersion.getVersion()), job.getJobId(), dataLimited, true,
      history, error);
  }

  private JobDataFragment getDataOnlyWithColumns(JobId jobId, ByteString batchSchema) {
    if (batchSchema == null) {
      return null;
    }
    BatchSchema schema = BatchSchema.deserialize(batchSchema);
    List<Column> columns =  JobDataFragmentWrapper.getColumnsFromSchema(schema).values().asList();
    return new JobDataFragment() {
      @Override
      public JobId getJobId() {
        return jobId;
      }

      @Override
      public List<Column> getColumns() {
        return columns;
      }

      @Override
      public List<Field> getFields() {
        return schema.getFields();
      }

      @Override
      public List<RecordBatchHolder> getRecordBatches() {
        return Collections.emptyList();
      }

      @Override
      public int getReturnedRowCount() {
        return 0;
      }

      @Override
      public Column getColumn(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String extractString(String column, int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object extractValue(String column, int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataType extractType(String column, int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {

      }
    };
  }

  InitialRunResponse createRunResponse(VirtualDatasetUI datasetUI, JobId jobId, DatasetVersion tipVersion) throws DatasetVersionNotFoundException, NamespaceException {
    final History history = getHistory(new DatasetPath(datasetUI.getFullPathList()), datasetUI.getVersion(), tipVersion);
    return new InitialRunResponse(newDataset(datasetUI, null), JobResource.getPaginationURL(jobId), jobId, history);
  }

  InitialPreviewResponse createPreviewResponse(DatasetPath path, Transformer.DatasetAndData datasetAndData, BufferAllocator allocator, int maxRecords, boolean catchExecutionError)
      throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {
    return createPreviewResponse(
      datasetAndData.getDataset(), datasetAndData.getJobData(), new DatasetVersionResourcePath(path, datasetAndData.getDataset().getVersion()),
      allocator, maxRecords, catchExecutionError);
  }

  InitialPreviewResponse createReviewResponse(DatasetPath datasetPath,
                                              VirtualDatasetUI newDataset,
                                              String jobId,
                                              DatasetVersion tipVersion,
                                              BufferAllocator allocator,
                                              Integer limit)
      throws DatasetVersionNotFoundException, NamespaceException, JobNotFoundException {

    JobDetails jobDetails;
    JobUI job;
    JobDataFragment dataLimited = null;
    boolean isApproximate;
    ApiErrorModel<?> error = null;

    try (AutoCloseables.RollbackCloseable cls = new AutoCloseables.RollbackCloseable(true)) {
      final JobDetailsRequest request = JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(new JobId(jobId)))
        .setUserName(username())
        .build();
      jobDetails = jobsService.getJobDetails(request);
      job = new JobUI(jobsService, new JobId(jobId), username());
      final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();
      QueryType queryType = jobInfo.getQueryType();
      isApproximate = queryType == QueryType.UI_PREVIEW || queryType == QueryType.UI_INTERNAL_PREVIEW || queryType == QueryType.UI_INITIAL_PREVIEW;

      JobState jobState = job.getJobAttempt().getState();
      switch (jobState) {
        case COMPLETED: {
          int finalLimit = limit == null ? INITIAL_RESULTSET_SIZE : limit;
          if (finalLimit > 0) {
            try {
              dataLimited = cls.add(job.getData().truncate(allocator, limit));
            } catch (Exception ex) {
              error = new ApiErrorModel<Void>(ApiErrorModel.ErrorType.INITIAL_PREVIEW_ERROR, ex.getMessage(), GenericErrorMessage.printStackTrace(ex), null);
            }
          } else {
            dataLimited = cls.add(getDataOnlyWithColumns(JobsProtoUtil.toStuff(jobDetails.getJobId()),
              JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getBatchSchema()));
          }
          break;
        }
        case FAILED: {
          com.dremio.dac.model.job.JobFailureInfo failureInfo = JobDetailsUI.toJobFailureInfo(jobInfo.getFailureInfo(), jobInfo.getDetailedFailureInfo());
          if (failureInfo.getMessage() != null) {
            switch (failureInfo.getType()) {
              case PARSE:
              case EXECUTION:
              case VALIDATION:
                error = new ApiErrorModel<>(ApiErrorModel.ErrorType.INVALID_QUERY, failureInfo.getMessage(), null,
                  new InvalidQueryException.Details(jobInfo.getSql(), ImmutableList.<String>of(), failureInfo.getErrors(), null));
                break;

              case UNKNOWN:
              default:
                error = new ApiErrorModel<Void>(ApiErrorModel.ErrorType.INITIAL_PREVIEW_ERROR, failureInfo.getMessage(), null, null);
            }
          }
          break;
        }
        case CANCELED: {
          // TODO(DX-14099): surface cancellation reason for initial preview
          break;
        }
        default:
          // nothing
          break;
      }
    } catch (DatasetVersionNotFoundException | NamespaceException | JobNotFoundException ex) {
      throw ex;
    } catch (Exception ex) {
      Throwables.throwIfUnchecked(ex);
      throw new RuntimeException(ex);
    }

    final History history = getHistory(datasetPath, newDataset.getVersion(), tipVersion);
    return InitialPreviewResponse.of(newDataset(newDataset, null), job.getJobId(), dataLimited, isApproximate,
      history, error);
  }

  public InitialPreviewResponse newUntitled(
      BufferAllocator allocator,
      FromBase from,
      DatasetVersion version,
      List<String> context,
      Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return newUntitled(allocator, from, version, context, null,false, limit);
  }

  private List<String> getParentDataset(FromBase from) {
    switch(from.wrap().getType()){
    case Table:
      return DatasetPath.defaultImpl(from.wrap().getTable().getDatasetPath()).toPathList();

    case SQL:
    case SubQuery:
    default:
      return null;
    }
  }

  /**
   * Check if UserException can be converted into {@code InvalidQueryException} and throw this exception
   * instead
   *
   * @param e
   * @param sql
   * @param context
   * @return the original exception if it cannot be converted into {@code InvalidQueryException}
   */
  public static UserException toInvalidQueryException(UserException e, String sql, List<String> context) {
    return toInvalidQueryException(e, sql, context, null);
  }

  /**
   * Check if UserException can be converted into {@code InvalidQueryException} and throw this exception
   * instead
   *
   * @param e
   * @param sql
   * @param context
   * @param datasetSummary
   * @return the original exception if it cannot be converted into {@code InvalidQueryException}
   */
  public static UserException toInvalidQueryException(UserException e, String sql, List<String> context,
                                                      DatasetSummary datasetSummary) {
    switch(e.getErrorType()) {
    case PARSE:
    case PLAN:
    case VALIDATION:
      String errorMessage = InvalidQueryErrorConverter.convert(e.getOriginalMessage());
      throw new InvalidQueryException(
          new InvalidQueryException.Details(
              sql,
              context,
              QueryError.of(e), datasetSummary), e, errorMessage);

      default:
        return e;
    }
  }

  /**
   * Create a new untitled dataset, and load preview data.
   *
   * @param from Source from where the dataset is created (can be a query or other dataset)
   * @param version Initial version of the new dataset
   * @param context Dataset context or current schema
   * @return
   * @throws DatasetNotFoundException
   * @throws DatasetVersionNotFoundException
   * @throws NamespaceException
   */
  public InitialPreviewResponse newUntitled(
    BufferAllocator allocator,
    FromBase from,
    DatasetVersion version,
    List<String> context,
    DatasetSummary parentSummary,
    boolean prepare,
    Integer limit)
      throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return newUntitled(allocator, from, version, context, parentSummary, prepare, limit, false);
  }

  public InitialPreviewResponse newUntitled(
    BufferAllocator allocator,
    FromBase from,
    DatasetVersion version,
    List<String> context,
    DatasetSummary parentSummary,
    boolean prepare,
    Integer limit,
    String engineName)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return newUntitled(allocator, from, version, context, parentSummary, prepare, limit, false, engineName);
  }
  /**
   * Create a new untitled dataset, and load preview data.
   *
   * @param from Source from where the dataset is created (can be a query or other dataset)
   * @param version Initial version of the new dataset
   * @param context Dataset context or current schema
   * @param runInSameThread runs metadata query in same AttemptManager thread
   * @return
   * @throws DatasetNotFoundException
   * @throws DatasetVersionNotFoundException
   * @throws NamespaceException
   */
  public InitialPreviewResponse newUntitled(
      BufferAllocator allocator,
      FromBase from,
      DatasetVersion version,
      List<String> context,
      DatasetSummary parentSummary,
      boolean prepare,
      Integer limit,
      boolean runInSameThread,
      String engineName)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {

    final VirtualDatasetUI newDataset = createNewUntitledMetadataOnly(from, version, context);
    final SqlQuery query = new SqlQuery(newDataset.getSql(), newDataset.getState().getContextList(), username(), engineName);

    try {
      final MetadataCollectingJobStatusListener listener = new MetadataCollectingJobStatusListener();
      final QueryType queryType = prepare ? QueryType.PREPARE_INTERNAL : QueryType.UI_PREVIEW;
      final JobData jobData = executor.runQueryWithListener(query, queryType, TMP_DATASET_PATH, newDataset.getVersion(), listener, runInSameThread);
      final QueryMetadata queryMetadata = listener.getMetadata();
      // get the job's info after the query metadata is available to make sure the schema has already been populated
      final JobDetails jobDetails = jobsService.getJobDetails(
        JobDetailsRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(jobData.getJobId()))
          .setUserName(query.getUsername())
          .build());
      final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();
      applyQueryMetaToDatasetAndSave(jobInfo, queryMetadata, newDataset, query, from);
      if(prepare) {
        limit = 0;
      }
      // in case of initial preview a returned dataset should be actual tip version. Dataset's path and version should
      // be consistent and represent actual key in dataset version store. So use dataset's path and
      // version as tipVersion
      return createPreviewResponse(newDataset, jobData, new DatasetVersionResourcePath(new DatasetPath(newDataset.getFullPathList()), newDataset.getVersion()),
        allocator, limit, false);
    } catch (Exception ex) {
      List<String> parentDataset = getParentDataset(from);

      if (ex instanceof UserException) {
        toInvalidQueryException((UserException) ex, query.getSql(), context, parentSummary);
      }

      // make sure we pass the parentSummary so that the UI can render edit original sql
      throw new NewDatasetQueryException(new NewDatasetQueryException.ExplorePageInfo(
        parentDataset, query.getSql(), context, newDataset(newDataset, null).getDatasetType(), parentSummary), ex);
    }
  }

  public InitialPreviewResponse newUntitled(
    BufferAllocator allocator,
    FromBase from,
    DatasetVersion version,
    List<String> context,
    DatasetSummary parentSummary,
    boolean prepare,
    Integer limit,
    boolean runInSameThread)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return newUntitled(allocator, from, version, context, parentSummary, prepare, limit, runInSameThread, null);
  }

    VirtualDatasetUI createNewUntitledMetadataOnly(FromBase from,
                                                DatasetVersion version,
                                                List<String> context) {
    final DatasetPath datasetPath = TMP_DATASET_PATH;
    final VirtualDatasetUI newDataset = newDatasetBeforeQueryMetadata(datasetPath, version, from.wrap(), context, username());
    newDataset.setLastTransform(new Transform(TransformType.createFromParent).setTransformCreateFromParent(new TransformCreateFromParent(from.wrap())));
    return newDataset;
  }

  InitialRunResponse newUntitledAndRun(FromBase from,
                                       DatasetVersion version,
                                       List<String> context,
                                       String engineName)
    throws DatasetNotFoundException, NamespaceException, DatasetVersionNotFoundException, InterruptedException {

    final VirtualDatasetUI newDataset = createNewUntitledMetadataOnly(from, version, context);
    final SqlQuery query = new SqlQuery(newDataset.getSql(), newDataset.getState().getContextList(), username(), engineName);

    newDataset.setLastTransform(new Transform(TransformType.createFromParent).setTransformCreateFromParent(new TransformCreateFromParent(from.wrap())));
    MetadataCollectingJobStatusListener listener = new MetadataCollectingJobStatusListener();

    try {
      final JobId jobId = executor.runQueryWithListener(query, QueryType.UI_RUN, TMP_DATASET_PATH, version, listener).getJobId();
      final QueryMetadata queryMetadata = listener.getMetadata();
      // get the job's info after the query metadata is available to make sure the schema has already been populated
      final JobDetails jobDetails = jobsService.getJobDetails(
        JobDetailsRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(jobId))
          .setUserName(username())
          .build());
      final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();
      applyQueryMetaToDatasetAndSave(jobInfo, queryMetadata, newDataset, query, from);
      return createRunResponse(newDataset, jobId, newDataset.getVersion());
    } catch(UserException e) {
      throw toInvalidQueryException(e, query.getSql(), context);
    } catch (JobNotFoundException e) {
      // should never be thrown
      UserException uex = UserException.systemError(e).buildSilently();
      throw toInvalidQueryException(uex, query.getSql(), context);
    }
  }

  InitialRunResponse newUntitledAndRun(FromBase from,
                                       DatasetVersion version,
                                       List<String> context)
    throws DatasetNotFoundException, NamespaceException, DatasetVersionNotFoundException, InterruptedException {
    return newUntitledAndRun(from, version, context, null);
  }

  private void applyQueryMetaToDatasetAndSave(JobInfo jobInfo, QueryMetadata queryMetadata,
      VirtualDatasetUI newDataset, SqlQuery query, FromBase from) throws DatasetNotFoundException, NamespaceException {
    QuerySemantics.populateSemanticFields(JobsProtoUtil.toStuff(queryMetadata.getFieldTypeList()), newDataset.getState());
    applyQueryMetadata(newDataset, jobInfo, queryMetadata);
    if (from.wrap().getType() == FromType.SQL) {
      newDataset.setState(QuerySemantics.extract(queryMetadata));
    }
    datasetService.putVersion(newDataset);
  }

  public static VirtualDatasetUI newDatasetBeforeQueryMetadata(
      DatasetPath datasetPath,
      DatasetVersion version,
      From from,
      List<String> sqlContext,
      String owner) {
    VirtualDatasetState dss = new VirtualDatasetState()
        .setFrom(from);
    dss.setContextList(sqlContext);
    VirtualDatasetUI vds = new VirtualDatasetUI();
    switch(from.getType()){
    case SQL:
      vds.setDerivation(Derivation.SQL);
      break;
    case Table:
      vds.setDerivation(Derivation.DERIVED_UNKNOWN);
      dss.setReferredTablesList(Arrays.asList(from.getTable().getAlias()));
      break;
    case SubQuery:
    default:
      vds.setDerivation(Derivation.UNKNOWN);
      dss.setReferredTablesList(Arrays.asList(from.getSubQuery().getAlias()));
      break;
    }

    vds.setOwner(owner);
    vds.setIsNamed(false);
    vds.setVersion(version);
    vds.setFullPathList(datasetPath.toPathList());
    vds.setName(datasetPath.getDataset().getName());
    vds.setState(dss);
    vds.setSql(SQLGenerator.generateSQL(dss));
    vds.setId(UUID.randomUUID().toString());
    vds.setContextList(sqlContext);

    // if we're doing a select * from table, and the context matches the base path of the table, let's avoid qualifying the table name.
    if(from.getType() == FromType.Table) {
      NamespaceKey path = new DatasetPath(from.getTable().getDatasetPath()).toNamespaceKey();
      if(path.getParent().getPathComponents().equals(sqlContext)) {
        vds.setSql(String.format("SELECT * FROM %s", SqlUtils.quoteIdentifier(path.getLeaf())));
      }
    }

    return vds;
  }

  /**
   * Get the history before a given version. This should only be used if this version is known to be
   * the last version in the history. Otherwise the other version of this method that takes a tip
   * version as well as a current version.
   *
   * @param datasetPath
   * @param currentDataset
   * @return
   * @throws DatasetVersionNotFoundException
   */
  History getHistory(final DatasetPath datasetPath, DatasetVersion currentDataset) throws DatasetVersionNotFoundException {
    return getHistory(datasetPath, currentDataset, currentDataset);
  }

  /**
   * Get the history for a given dataset path, starting at a given version to
   * treat as the tip of the history.
   *
   * The current version is also passed because it can trail behind the tip of the
   * history if a user selects a previous point in the history. We still want to
   * show the future history items to allow them to navigate "Back to the Future" (TM).
   *
   * @param datasetPath the dataset path of the version at the tip of the history
   * @param versionToMarkCurrent the version currently selected in the client
   * @param tipVersion the latest history item known, which may be passed the selected versionToMarkCurrent,
   *                   this can be null and the tip will be assumed to be the versionToMarkCurrent the
   *                   same behavior as the version of this method that lacks the tipVersion entirely
   * @return
   * @throws DatasetVersionNotFoundException
   */
  History getHistory(final DatasetPath datasetPath, final DatasetVersion versionToMarkCurrent, DatasetVersion tipVersion) throws DatasetVersionNotFoundException {
    // while the current callers of this method all do their own null guarding, adding this defensively for
    // future callers that may fail to handle this case
    tipVersion = tipVersion != null ? tipVersion : versionToMarkCurrent;

    final List<HistoryItem> historyItems = new ArrayList<>();
    VirtualDatasetUI currentDataset = null;
    DatasetVersion currentVersion = tipVersion;
    DatasetPath currentPath = datasetPath;
    NameDatasetRef previousVersion;
    try {
      do {
        currentDataset = datasetService.getVersion(currentPath, currentVersion);
        DatasetVersionResourcePath versionedResourcePath =
          new DatasetVersionResourcePath(currentPath, currentVersion);

        // grab the most recent job for this dataset version (note the use of limit 1 to avoid
        // retrieving all results, the API just returns a list, so this also has to index into the returned list
        // that will always contain a single element)
        final SearchJobsRequest request = SearchJobsRequest.newBuilder()
          .setDataset(VersionedDatasetPath.newBuilder()
            .addAllPath(currentDataset.getFullPathList())
            .setVersion(currentDataset.getVersion().getVersion())
            .build())
          .setLimit(1)
          .build();
        Iterable<JobSummary> jobSummaries = jobsService.searchJobs(request);
        final JobState jobState;
        // jobs are not persisted forever so we may not have a job for this version of the dataset
        Iterator<JobSummary> iterator = jobSummaries.iterator();
        if (iterator.hasNext()) {
          jobState = JobsProtoUtil.toStuff(iterator.next().getJobState());
        } else {
          jobState = JobState.COMPLETED;
        }
        historyItems.add(
          new HistoryItem(versionedResourcePath, jobState,
            TransformBase.unwrap(currentDataset.getLastTransform()).accept(new DescribeTransformation()), username(),
            currentDataset.getCreatedAt(), 0L, true, null, null));

        previousVersion = currentDataset.getPreviousVersion();
        if (previousVersion != null) {
          currentVersion = new DatasetVersion(previousVersion.getDatasetVersion());
          currentPath = new DatasetPath(previousVersion.getDatasetPath());
        }
      } while (previousVersion != null);
    } catch (DatasetNotFoundException e) {
      // If for some reason the history chain is broken/corrupt, we will get an DatasetNotFoundException.  If we have a
      // partial history, we return it.  If no history items are found, rethrow the exception.
      if (currentDataset == null) {
        throw e;
      }

      logger.warn("Dataset history for [{}] and tip version [{}] is broken at path [{}] and version [{}]", datasetPath,
        tipVersion, currentPath, currentVersion);
    }

    Collections.reverse(historyItems);

    // isEdited
    DatasetVersion savedVersion = null;
    try {
      savedVersion = datasetService.get(datasetPath).getVersion();
    } catch (DatasetNotFoundException | NamespaceException e) {
      // do nothing
    }

    boolean isEdited = savedVersion == null ?
        currentDataset.getDerivation() == Derivation.SQL || historyItems.size() > 1 :
        !tipVersion.equals(savedVersion);

    return new History(historyItems, versionToMarkCurrent, currentDataset.getVersion().getValue(), isEdited);
  }

  /**
   * When we save a version that previously lacked a name, or we are changing the name of a
   * dataset, we go back and update the data path for all untitled versions stored throughout
   * the history.
   *
   * In the case of a version that did have a name other than untitled, the most recent history
   * item will be saved again with the requested name because finding a history item currently
   * requires a version number and dataset path.
   *
   * @param versionToSave the dataset path of the version at the tip of the history
   * @param newPath       the new path to copy throughout the history
   * @throws DatasetVersionNotFoundException
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  void rewriteHistory(final VirtualDatasetUI versionToSave, final DatasetPath newPath)
      throws DatasetVersionNotFoundException, DatasetNotFoundException, NamespaceException {

    DatasetVersion previousDatasetVersion;
    DatasetPath previousPath;
    NameDatasetRef previousVersion;
    VirtualDatasetUI currentDataset = versionToSave;
    boolean previousVersionRequiresRename;
    // Rename the last history item, and all previous history items that are unnamed.
    // The loop terminates when hitting the end of the history or a named history item, this
    // means that the history for one dataset can contain history items that are stored
    // with a name it had previously.
    do {
      previousVersion = currentDataset.getPreviousVersion();
      // change the path in this history item to the new one, will be peristed below
      // after possibly changing the link the the previous version if it will also be renamed
      currentDataset.setFullPathList(newPath.toPathList());
      if (previousVersion != null) {
        previousPath = new DatasetPath(previousVersion.getDatasetPath());
        previousDatasetVersion = new DatasetVersion(previousVersion.getDatasetVersion());
        previousVersionRequiresRename = previousPath.equals(TMP_DATASET_PATH);
        if (previousVersionRequiresRename) {
          // create a new link to the previous dataset with a changed dataset path
          NameDatasetRef prev = new NameDatasetRef()
              .setDatasetPath(newPath.toPathString())
              .setDatasetVersion(previousVersion.getDatasetVersion());
          currentDataset.setPreviousVersion(prev);
          currentDataset.setName(newPath.getDataset().getName());
          datasetService.putVersion(currentDataset);
          currentDataset = datasetService.getVersion(previousPath, previousDatasetVersion);
        } else {
          datasetService.putVersion(currentDataset);
        }
      } else {
        previousVersionRequiresRename = false;
        datasetService.putVersion(currentDataset);
      }
    } while (previousVersionRequiresRename);
  }

  public static void applyQueryMetadata(VirtualDatasetUI dataset, JobInfo jobInfo, QueryMetadata metadata) {
    applyQueryMetadata(dataset,
      Optional.fromNullable(jobInfo.getParentsList()),
      Optional.fromNullable(jobInfo.getBatchSchema()).transform((b) -> BatchSchema.deserialize(b)),
      Optional.fromNullable(jobInfo.getFieldOriginsList()),
      Optional.fromNullable(jobInfo.getGrandParentsList()),
      metadata);
  }

  public static void applyQueryMetadata(VirtualDatasetUI dataset, Optional<List<ParentDatasetInfo>> parents,
      Optional<BatchSchema> batchSchema, Optional<List<FieldOrigin>> fieldOrigins, Optional<List<ParentDataset>> grandParents,
      QueryMetadata metadata) {
    final List<ViewFieldType> viewFieldTypesList = JobsProtoUtil.toStuff(metadata.getFieldTypeList());
    dataset.setCalciteFieldsList(viewFieldTypesList);
    if (batchSchema.isPresent()) {
      dataset.setSqlFieldsList(ViewFieldsHelper.getBatchSchemaFields(batchSchema.get()));
      dataset.setRecordSchema(batchSchema.get().toByteString());
    } else {
      dataset.setSqlFieldsList(viewFieldTypesList);
    }

    if (parents.isPresent()) {
      List<ParentDataset> otherParents = new ArrayList<>();
      for(ParentDatasetInfo parent : parents.get()){
        otherParents.add(new ParentDataset()
            .setDatasetPathList(parent.getDatasetPathList())
            .setType(parent.getType())
            .setLevel(1));
      }
      dataset.setParentsList(otherParents);
    }
    if (fieldOrigins.isPresent()) {
      dataset.setFieldOriginsList(fieldOrigins.get());
    }
    if (grandParents.isPresent()) {
      dataset.setGrandParentsList(grandParents.get());
    }
    updateDerivationAfterLearningOriginsAndAncestors(dataset);
  }

  private static void updateDerivationAfterLearningOriginsAndAncestors(VirtualDatasetUI newDataset){
    // only resolve if we need to, otherwise we should leave the previous derivation alone. (e.g. during a transform)
    if(newDataset.getDerivation() != Derivation.DERIVED_UNKNOWN){
      return;
    }

    // if we have don't have one parent, we must have had issues detecting parents of SQL we generated, fallback.
    if(newDataset.getParentsList() != null && newDataset.getParentsList().size() != 1){
      newDataset.setDerivation(Derivation.UNKNOWN);
      return;
    }

    final Set<List<String>> origins = new HashSet<>();
    for(FieldOrigin col : listNotNull(newDataset.getFieldOriginsList())) {
      for(Origin colOrigin : listNotNull(col.getOriginsList())){
        origins.add(colOrigin.getTableList());
      }
    }

    // logic: if we have a single parent and that parent is also the only
    // table listed in field origins, then we are derived from a physical
    // dataset. Otherwise, we are a virtual dataset.
    if(origins.size() == 1 && origins.iterator().next().equals(newDataset.getParentsList().get(0).getDatasetPathList())){
      newDataset.setDerivation(Derivation.DERIVED_PHYSICAL);
    } else {
      newDataset.setDerivation(Derivation.DERIVED_VIRTUAL);
    }

  }

  protected DatasetUI newDataset(VirtualDatasetUI vds, DatasetVersion tipVersion) throws NamespaceException {
    return DatasetUI.newInstance(vds, tipVersion, datasetService.getNamespaceService());
  }

}
