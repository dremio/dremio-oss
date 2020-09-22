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
package com.dremio.service.jobs;

import static com.dremio.exec.planner.sql.SqlExceptionHelper.END_COLUMN_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.END_LINE_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.START_COLUMN_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.START_LINE_CONTEXT;
import static com.dremio.service.jobs.JobIndexKeys.JOB_STATE;

import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.parquet.ParquetWriter;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.StoreJobResultRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

/**
 * utility class.
 */
public final class JobsServiceUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobsServiceUtil.class);

  private static final String ACCELERATOR_STORAGEPLUGIN_NAME = "__accelerator";

  private JobsServiceUtil() {
  }

  public static final ImmutableSet<JobState> finalJobStates =
    Sets.immutableEnumSet(JobState.CANCELED, JobState.COMPLETED, JobState.FAILED);

  public static final ImmutableSet<JobState> nonFinalJobStates =
    ImmutableSet.copyOf(Sets.difference(EnumSet.allOf(JobState.class), finalJobStates));

  private static final SearchQuery apparentlyAbandonedQuery;

  static {
    apparentlyAbandonedQuery = SearchQueryUtils.or(
      nonFinalJobStates.stream()
        .map(input -> SearchQueryUtils.newTermQuery(JOB_STATE, input.name()))
        .collect(Collectors.toList())
      );
  }

  static SearchQuery getApparentlyAbandonedQuery() {
    return apparentlyAbandonedQuery;
  }

  static boolean isNonFinalState(JobState jobState) {
    return jobState == null || !finalJobStates.contains(jobState);
  }

  static NodeEndpoint toStuff(CoordinationProtos.NodeEndpoint pb) {
    // TODO use schemas to do this...
    NodeEndpoint ep = new NodeEndpoint();
    ProtobufIOUtil.mergeFrom(pb.toByteArray(), ep, NodeEndpoint.getSchema());
    return ep;
  }

  static CoordinationProtos.NodeEndpoint toPB(NodeEndpoint stuf) {
    // TODO use schemas to do this...
    LinkedBuffer buffer = LinkedBuffer.allocate();
    byte[] bytes = ProtobufIOUtil.toByteArray(stuf, stuf.cachedSchema(), buffer);
    try {
      return CoordinationProtos.NodeEndpoint.PARSER.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Cannot convert from protostuff to protobuf");
    }
  }

  /**
   * Translate job id to external id.
   *
   * @param jobId job id
   * @return external id
   */
  static ExternalId getJobIdAsExternalId(JobId jobId) {
    UUID id = UUID.fromString(jobId.getId());
    return ExternalId.newBuilder()
        .setPart1(id.getMostSignificantBits())
        .setPart2(id.getLeastSignificantBits())
        .build();
  }

  /**
   * Translate external id to job id.
   *
   * @param id external id
   * @return job id
   */
  public static JobId getExternalIdAsJobId(ExternalId id) {
    return new JobId(new UUID(id.getPart1(), id.getPart2()).toString());
  }

  /**
   * Returns job status, given query state.
   *
   * @param state query state
   * @return job status
   */
  static JobState queryStatusToJobStatus(QueryState state) {
    switch (state) {
    case STARTING:
      return JobState.STARTING;
    case RUNNING:
      return JobState.RUNNING;
    case ENQUEUED:
      return JobState.ENQUEUED;
    case COMPLETED:
      return JobState.COMPLETED;
    case CANCELED:
      return JobState.CANCELED;
    case FAILED:
      return JobState.FAILED;
    default:
      return JobState.NOT_SUBMITTED;
    }
  }

  /**
   * Returns job status, given attempt state.
   *
   * @param state attempt state
   * @return job status
   */
  static JobState attemptStatusToJobStatus(AttemptEvent.State state) {
    switch (state) {
      case METADATA_RETRIEVAL:
        return com.dremio.service.job.proto.JobState.METADATA_RETRIEVAL;
      case STARTING:
        return com.dremio.service.job.proto.JobState.STARTING;
      case PLANNING:
        return com.dremio.service.job.proto.JobState.PLANNING;
      case RUNNING:
        return com.dremio.service.job.proto.JobState.RUNNING;
      case COMPLETED:
        return com.dremio.service.job.proto.JobState.COMPLETED;
      case CANCELED:
        return com.dremio.service.job.proto.JobState.CANCELED;
      case FAILED:
        return com.dremio.service.job.proto.JobState.FAILED;
      case QUEUED:
        return com.dremio.service.job.proto.JobState.QUEUED;
      case PENDING:
        return com.dremio.service.job.proto.JobState.PENDING;
      case ENGINE_START:
        return com.dremio.service.job.proto.JobState.ENGINE_START;
      case EXECUTION_PLANNING:
        return com.dremio.service.job.proto.JobState.EXECUTION_PLANNING;
      default:
        return com.dremio.service.job.proto.JobState.INVALID_STATE;
    }
  }

  /**
   * convert proto to beans external state
   *
   * @param state external state
   * @return external status
   */
  static com.dremio.exec.proto.beans.AttemptEvent.State convertAttemptStatus(AttemptEvent.State state) {
    switch (state) {
      case METADATA_RETRIEVAL:
        return com.dremio.exec.proto.beans.AttemptEvent.State.METADATA_RETRIEVAL;
      case STARTING:
        return com.dremio.exec.proto.beans.AttemptEvent.State.STARTING;
      case PLANNING:
        return com.dremio.exec.proto.beans.AttemptEvent.State.PLANNING;
      case RUNNING:
        return com.dremio.exec.proto.beans.AttemptEvent.State.RUNNING;
      case COMPLETED:
        return com.dremio.exec.proto.beans.AttemptEvent.State.COMPLETED;
      case CANCELED:
        return com.dremio.exec.proto.beans.AttemptEvent.State.CANCELED;
      case FAILED:
        return com.dremio.exec.proto.beans.AttemptEvent.State.FAILED;
      case QUEUED:
        return com.dremio.exec.proto.beans.AttemptEvent.State.QUEUED;
      case PENDING:
        return com.dremio.exec.proto.beans.AttemptEvent.State.PENDING;
      case ENGINE_START:
        return com.dremio.exec.proto.beans.AttemptEvent.State.ENGINE_START;
      case EXECUTION_PLANNING:
        return com.dremio.exec.proto.beans.AttemptEvent.State.EXECUTION_PLANNING;
      default:
        return com.dremio.exec.proto.beans.AttemptEvent.State.INVALID_STATE;
    }
  }

  /**
   * Returns a list of partitions into which CTAS files got written.
   */
  static List<String> getPartitions(final PlanningSet planningSet) {
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    // visit every single major fragment and check to see if there is a PDFSWriter
    // if so add address of every minor fragment as a data partition to builder.
    for (final Wrapper majorFragment : planningSet) {
      majorFragment.getNode().getRoot().accept(new AbstractPhysicalVisitor<Void, Void, RuntimeException>() {
        @Override
        public Void visitOp(final PhysicalOperator op, Void value) throws RuntimeException {
          // override to prevent throwing exception, super class throws an exception
          visitChildren(op, value);
          return null;
        }

        @Override
        public Void visitWriter(final Writer writer, Void value) throws RuntimeException {
          // we only want to get partitions for the lower writer, since this is the actual data
          // there may be a second writer that writes the metadata "results", but we don't care about that one
          super.visitWriter(writer, null);
          // TODO DX-5438: Remove PDFS specific code
          if (writer instanceof ParquetWriter
              && ACCELERATOR_STORAGEPLUGIN_NAME.equals(((ParquetWriter) writer).getPluginId().getName())
              && ((ParquetWriter) writer).isPdfs()) {
            final List<String> addresses = Lists.transform(majorFragment.getAssignedEndpoints(),
              CoordinationProtos.NodeEndpoint::getAddress);
            builder.addAll(addresses);
          }
          return null;
        }
      }, null);
    }

    return ImmutableList.copyOf(builder.build());
  }

  static JobFailureInfo toFailureInfo(String verboseError) {
    // TODO: Would be easier if profile had structured error too
    String[] lines = verboseError.split("\n");
    if (lines.length < 3) {
      return null;
    }
    final JobFailureInfo.Type type;
    final String message;

    try {
      Object[] result = new MessageFormat("{0} ERROR: {1}").parse(lines[0]);

      String errorTypeAsString = (String) result[0];
      ErrorType errorType;
      try {
        errorType = ErrorType.valueOf(errorTypeAsString);
      } catch(IllegalArgumentException e) {
        errorType = null;
      }

      if (errorType != null) {
        switch(errorType) {
        case PARSE:
          type = JobFailureInfo.Type.PARSE;
          break;

        case PLAN:
          type = JobFailureInfo.Type.PLAN;
          break;

        case VALIDATION:
          type = JobFailureInfo.Type.VALIDATION;
          break;

        case FUNCTION:
          type = JobFailureInfo.Type.EXECUTION;
          break;

        default:
          type = JobFailureInfo.Type.UNKNOWN;
        }
      } else {
        type = JobFailureInfo.Type.UNKNOWN;
      }

      message = (String) result[1];
    } catch (ParseException e) {
      LOGGER.warn("Cannot parse error message {}", lines[0], e);
      return null;
    }

    List<JobFailureInfo.Error> errors;
    JobFailureInfo.Error error = new JobFailureInfo.Error()
      .setMessage(message);
    if (lines.length > 3) {
      // Parse all the context lines
      Map<String, String> context = new HashMap<>();
      for (int i = 3; i < lines.length; i++) {
        String line = lines[i];
        if (line.isEmpty()) {
          break;
        }

        String[] contextLine = line.split(" ", 2);
        if (contextLine.length < 2) {
          continue;
        }

        context.put(contextLine[0], contextLine[1]);
      }

      if (context.containsKey(START_LINE_CONTEXT)) {
        try {
          int startLine = Integer.parseInt(context.get(START_LINE_CONTEXT));
          int startColumn = Integer.parseInt(context.get(START_COLUMN_CONTEXT));
          int endLine = Integer.parseInt(context.get(END_LINE_CONTEXT));
          int endColumn = Integer.parseInt(context.get(END_COLUMN_CONTEXT));

          error
            .setStartLine(startLine)
            .setStartColumn(startColumn)
            .setEndLine(endLine)
            .setEndColumn(endColumn);
        } catch (NullPointerException | NumberFormatException e) {
          // Ignoring
        }
      }
    }
    errors = ImmutableList.of(error);

    return new JobFailureInfo().setMessage("Invalid Query Exception").setType(type).setErrorsList(errors);
  }

  private static JobInfo getJobInfo(StoreJobResultRequest request) {
    return new JobInfo()
      .setJobId(JobsProtoUtil.toStuff(request.getJobId()))
      .setSql(request.getSql())
      .setRequestType(JobsProtoUtil.toStuff(request.getRequestType()))
      .setUser(request.getUser())
      .setStartTime(request.getStartTime())
      .setFinishTime(request.getFinishTime())
      .setDatasetPathList(request.getDataset().getPathList())
      .setDatasetVersion(request.getDataset().getVersion())
      .setQueryType(JobsProtoUtil.toStuff(request.getQueryType()))
      .setDescription(request.getDescription())
      .setOriginalCost(request.getOriginalCost())
      .setOutputTableList(request.getOutputTableList());
  }

  private static List<JobAttempt> getAttempts(StoreJobResultRequest request) {
    final ArrayList<JobAttempt> jobAttempts = new ArrayList<>();
    jobAttempts.add(new JobAttempt()
      .setState(JobsProtoUtil.toStuff(request.getJobState()))
      .setInfo(getJobInfo(request))
      .setAttemptId(request.getAttemptId())
      .setEndpoint(JobsProtoUtil.toStuff(request.getEndpoint()))
      .setStateListList(JobsProtoUtil.toBuf2(request.getStateListList())));

    return jobAttempts;
  }

  static JobResult toJobResult(StoreJobResultRequest request) {
    return new JobResult()
      .setAttemptsList(getAttempts(request))
      .setCompleted(request.getJobState() == com.dremio.service.job.JobState.COMPLETED);
  }
  static JobSummary toJobSummary(Job job) {

    final JobAttempt firstJobAttempt = job.getAttempts().get(0);
    final JobInfo firstJobAttemptInfo = firstJobAttempt.getInfo();
    final JobAttempt lastJobAttempt = job.getJobAttempt();
    final JobInfo lastJobAttemptInfo = lastJobAttempt.getInfo();
    final List<com.dremio.exec.proto.beans.AttemptEvent> stateList;
    synchronized (lastJobAttempt) {
      if (lastJobAttempt.getStateListList() == null) {
        stateList = new ArrayList<>();
      } else {
        stateList = new ArrayList<>(lastJobAttempt.getStateListList());
      }
    }

    JobSummary.Builder jobSummaryBuilder = JobSummary.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
      .setJobState(JobsProtoUtil.toBuf(lastJobAttempt.getState()))
      .addAllStateList(JobsProtoUtil.toStuff2(stateList))
      .setUser(firstJobAttemptInfo.getUser())
      .addAllDatasetPath(lastJobAttemptInfo.getDatasetPathList())
      .setRequestType(JobsProtoUtil.toBuf(lastJobAttemptInfo.getRequestType()))
      .setQueryType(JobsProtoUtil.toBuf(lastJobAttemptInfo.getQueryType()))
      .setAccelerated(lastJobAttemptInfo.getAcceleration() != null)
      .setDatasetVersion(firstJobAttemptInfo.getDatasetVersion())
      .setSnowflakeAccelerated(false)
      .setSpilled(lastJobAttemptInfo.getSpillJobDetails() != null)
      .setSql(lastJobAttemptInfo.getSql())
      .setNumAttempts(job.getAttempts().size())
      .setRecordCount(job.getRecordCount());

    if (lastJobAttempt.getStats() != null && lastJobAttempt.getStats().getIsOutputLimited() != null) {
      jobSummaryBuilder.setOutputLimited(lastJobAttempt.getStats().getIsOutputLimited());
    }

    if (lastJobAttempt.getDetails() != null && lastJobAttempt.getDetails().getOutputRecords() != null) {
      jobSummaryBuilder.setOutputRecords(lastJobAttempt.getDetails().getOutputRecords());
    }

    if (lastJobAttemptInfo.getFailureInfo() != null) {
      jobSummaryBuilder.setFailureInfo(lastJobAttemptInfo.getFailureInfo());
    }

    if (firstJobAttemptInfo.getStartTime() != null) {
      jobSummaryBuilder.setStartTime(firstJobAttemptInfo.getStartTime());
    }

    if (lastJobAttemptInfo.getFinishTime() != null) {
      jobSummaryBuilder.setEndTime(lastJobAttemptInfo.getFinishTime());
    }

    if (lastJobAttemptInfo.getDescription() != null) {
      jobSummaryBuilder.setDescription(lastJobAttemptInfo.getDescription());
    }

    JobProtobuf.JobFailureInfo detailedJobFailureInfo = JobsProtoUtil.toBuf(lastJobAttemptInfo.getDetailedFailureInfo());
    if (detailedJobFailureInfo != null) {
      jobSummaryBuilder.setDetailedJobFailureInfo(detailedJobFailureInfo);
    }

    JobProtobuf.JobCancellationInfo jobCancellationInfo = JobsProtoUtil.toBuf(lastJobAttemptInfo.getCancellationInfo());
    if (jobCancellationInfo != null) {
      jobSummaryBuilder.setCancellationInfo(jobCancellationInfo);
    }

    ParentDatasetInfo parentDatasetInfo = null;
    if (lastJobAttemptInfo.getParentsList() != null && lastJobAttemptInfo.getParentsList().size() > 0) {
      parentDatasetInfo = lastJobAttemptInfo.getParentsList().get(0);
    }
    if (parentDatasetInfo != null) {
      jobSummaryBuilder.setParent(JobsProtoUtil.toBuf(parentDatasetInfo));
    }

    return jobSummaryBuilder.build();
  }

  static JobDetails toJobDetails(Job job, boolean provideResultInfo) {
    final JobDetails.Builder jobDetailsBuilder = JobDetails.newBuilder();
    int numAttempts = job.getAttempts().size();
    for (int i = 0; i <  numAttempts; ++i) {
      JobAttempt attempt = job.getAttempts().get(i);
      synchronized (attempt) {
        jobDetailsBuilder.addAttempts(JobsProtoUtil.toBuf(attempt));
      }
    }
    jobDetailsBuilder
      .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
      .setCompleted(job.isCompleted());

    if (provideResultInfo && job.getJobAttempt().getState() == JobState.COMPLETED) {
      final boolean hasResults = job.hasResults();
      jobDetailsBuilder.setHasResults(hasResults);

      // Gets JobResultsTableName from JobData, but only load if job is completed
      if (hasResults || job.isInternal()) {
        jobDetailsBuilder.setJobResultTableName(job.getData().getJobResultsTable());
      }
    }
    return jobDetailsBuilder.build();
  }

  public static JobTypeStats.Types toType(JobStats.Type type) {
    switch (type) {
    case UI:
      return JobTypeStats.Types.UI;
    case EXTERNAL:
      return JobTypeStats.Types.EXTERNAL;
    case ACCELERATION:
      return JobTypeStats.Types.ACCELERATION;
    case DOWNLOAD:
      return JobTypeStats.Types.DOWNLOAD;
    case INTERNAL:
      return JobTypeStats.Types.INTERNAL;
    default:
    case UNRECOGNIZED:
      throw new IllegalArgumentException();
    }
  }

  /**
   * Creates JobInfo from SubmitJobRequest
   */
  public static JobInfo createJobInfo(SubmitJobRequest jobRequest, JobId jobId, String inSpace) {
    final JobInfo jobInfo = new JobInfo(jobId, jobRequest.getSqlQuery().getSql(),
      jobRequest.getVersionedDataset().getVersion(), JobsProtoUtil.toStuff(jobRequest.getQueryType()))
      .setSpace(inSpace)
      .setUser(jobRequest.getUsername())
      .setStartTime(System.currentTimeMillis())
      .setDatasetPathList(jobRequest.getVersionedDataset().getPathList())
      .setResultMetadataList(new ArrayList<ArrowFileMetadata>())
      .setContextList(jobRequest.getSqlQuery().getContextList());

    if (jobRequest.hasDownloadSettings()) {
      jobInfo.setDownloadInfo(new DownloadInfo()
        .setDownloadId(jobRequest.getDownloadSettings().getDownloadId())
        .setFileName(jobRequest.getDownloadSettings().getFilename()));
    } else if (jobRequest.hasMaterializationSettings()) {
      jobInfo.setMaterializationFor(JobsProtoUtil.toStuff(jobRequest.getMaterializationSettings().getMaterializationSummary()));
    }
    return jobInfo;
  }
}
