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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.exec.store.parquet.ParquetWriter;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
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

  /**
   * Waits for a given job's completion
   */
  public static Job waitForJobCompletion(CompletableFuture<Job> jobFuture) {
    final Job job = Futures.getUnchecked(jobFuture);
    job.getData().loadIfNecessary();
    return job;
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
}
