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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.datastore.SearchTypes;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.exec.store.Views;
import com.dremio.proto.model.attempts.RequestType;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubstitutionSettings;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobCancellationInfo;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.metadata.QuerySemantics;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.jobs.metadata.proto.VirtualDatasetState;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;

/**
 * Public utility class for Protobuf and Protostuff conversions in JobsService and for clients.
 */
public final class JobsProtoUtil {

  private JobsProtoUtil() {}

  /**
   * Get the last attempt of a JobDetails response as Protostuff
   * @param jobDetails JobDetails to get last attempt from
   * @return JobAttempt Protostuff corresponding to the last attempt
   */
  public static JobAttempt getLastAttempt(com.dremio.service.job.JobDetails jobDetails) {
    Preconditions.checkState(jobDetails.getAttemptsCount() >=1, "There should be at least one attempt in Job");
    return toStuff(jobDetails.getAttempts(jobDetails.getAttemptsCount() - 1));
  }

  /**
   * Generic method to convert Protostuff to Protobuf. Uses LegacyProtobufSerializer because deserializing with the
   * regular protobuf serializer does not handle repeated fields correctly.
   * @param protobufParser Parser for protobuf object
   * @param protostuff Protostuff object to convert
   * @param <M> Type of Protobuf
   * @param <T> Type of Protobuff
   * @return Converted object as Protobuf
   */
  private static <M extends GeneratedMessageV3, T extends Message<T> & Schema<T>>
  M toBuf(Parser<M> protobufParser, T protostuff) {
    try {
      LinkedBuffer buffer = LinkedBuffer.allocate();
      byte[] bytes = ProtobufIOUtil.toByteArray(protostuff, protostuff.cachedSchema(), buffer);
      // LegacyProtobufSerializer is necessary as it deals with stuff/buf grouping differences
      return LegacyProtobufSerializer.parseFrom(protobufParser, bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Cannot convert from protostuff to protobuf");
    }
  }

  /**
   * Generic method to convert Protobuf to Protostuff. Safe to use Protostuff's serializer as it can properly
   * deserialize messages serialized by Protobuf.
   * @param protostuffSchema Protostuff object schema
   * @param protobuf Protobuf object to convert
   * @param <M> Type of Protobuf
   * @param <T> Type of Protostuff
   * @return Converted object as Protostuff
   */
  private static <M extends GeneratedMessageV3, T extends Message<T> & Schema<T>>
  T toStuff(Schema<T> protostuffSchema, M protobuf) {
    T message = protostuffSchema.newMessage();
    ProtobufIOUtil.mergeFrom(protobuf.toByteArray(), message, protostuffSchema);
    return message;
  }

  /**
   * Convert JobAttempt Protostuff to Protobuf
   */
  public static JobProtobuf.JobAttempt toBuf(JobAttempt attempt) {
    return JobsProtoUtil.toBuf(JobProtobuf.JobAttempt.getDefaultInstance().getParserForType(), attempt);
  }

  /**
   * Convert JobAttempt Protobuf to Protostuff
   */
  public static JobAttempt toStuff(JobProtobuf.JobAttempt attempt) {
    return JobsProtoUtil.toStuff(JobAttempt.getDefaultInstance().cachedSchema(), attempt);
  }

  /**
   * Convert JobId Protostuff to Protobuf
   */
  public static JobProtobuf.JobId toBuf(JobId jobId) {
    return JobsProtoUtil.toBuf(JobProtobuf.JobId.getDefaultInstance().getParserForType(), jobId);
  }

  /**
   * Convert ViewFieldType Protostuff to Protobuf
   */
  public static DatasetCommonProtobuf.ViewFieldType toBuf(ViewFieldType viewFieldType) {
    return toBuf(DatasetCommonProtobuf.ViewFieldType.getDefaultInstance().getParserForType(), viewFieldType);
  }

  /**
   * Convert List of ViewFieldType Protostuff to Protobuf
   */
  public static List<DatasetCommonProtobuf.ViewFieldType> toBuf(List<ViewFieldType> viewFieldTypes) {
    return viewFieldTypes.stream()
      .map(JobsProtoUtil::toBuf)
      .collect(Collectors.toList());
  }

  /**
   * Convert ViewFieldType to Protostuff
   */
  public static ViewFieldType toStuff(DatasetCommonProtobuf.ViewFieldType viewFieldType) {
    return toStuff(ViewFieldType.getSchema(), viewFieldType);
  }

  /**
   * Convert List of ViewFieldType to Protostuff
   */
  public static List<ViewFieldType> toStuff(List<DatasetCommonProtobuf.ViewFieldType> fieldTypes) {
    return fieldTypes.stream()
      .map(JobsProtoUtil::toStuff)
      .collect(Collectors.toList());
  }

  /**
   * Convert JobId Protobuf to Protostuff
   */
  public static JobId toStuff(JobProtobuf.JobId jobId) {
    return JobsProtoUtil.toStuff(JobId.getDefaultInstance().cachedSchema(), jobId);
  }

  public static JobProtobuf.JobFailureInfo toBuf(JobFailureInfo jobFailureInfo) {
    if (jobFailureInfo == null) {
      return null;
    }
    return JobsProtoUtil.toBuf(JobProtobuf.JobFailureInfo.getDefaultInstance().getParserForType(), jobFailureInfo);
  }

  /**
   * Converts protobuf JobFailureInfo to protostuf JobFailureInfo to reuse POJO converter
   */
  public static JobFailureInfo toStuff(JobProtobuf.JobFailureInfo jobFailureInfo) {
    return JobsProtoUtil.toStuff(JobFailureInfo.getDefaultInstance().cachedSchema(), jobFailureInfo);
  }

  public static JobResult toStuff(JobProtobuf.JobResult jobResult) {
    return JobsProtoUtil.toStuff(JobResult.getDefaultInstance().cachedSchema(), jobResult);
  }

  /***
   * Converts pojo QueryMetadata to protobuf QueryMetadata
   */
  public static QueryMetadata toBuf(com.dremio.service.jobs.metadata.QueryMetadata queryMetadata) {
    final QueryMetadata.Builder builder = QueryMetadata.newBuilder();
    final List<ViewFieldType> viewFieldTypes = Views.viewToFieldTypes(Views.relDataTypeToFieldType(queryMetadata.getRowType()));
    builder.addAllFieldType(toBuf(viewFieldTypes));
    final Optional<VirtualDatasetState> state = QuerySemantics.extract(queryMetadata);
    if (state.isPresent()) {
      builder.setState(state.get());
    }
    return builder.build();
  }

  /***
   * Converts protostuff JobCancellationInfo to protobuf JobCancellationInfo to store in JobSummary
   */
  static JobProtobuf.JobCancellationInfo toBuf(JobCancellationInfo jobCancellationInfo) {
    if (jobCancellationInfo == null) {
      return null;
    }
    return JobsProtoUtil.toBuf(JobProtobuf.JobCancellationInfo.getDefaultInstance().getParserForType(), jobCancellationInfo);
  }

  /**
   * Converts protobuf JobCancellationInfo to protostuf JobCancellationInfo to reuse POJO converter
   */
  public static JobCancellationInfo toStuff(JobProtobuf.JobCancellationInfo jobCancellationInfo) {
    return JobsProtoUtil.toStuff(JobCancellationInfo.getDefaultInstance().cachedSchema(), jobCancellationInfo);
  }

  /***
   * Converts protostuff ParentDatasetInfo to protobuf ParentdatasetInfo to store in JobSummary
   */
  static JobProtobuf.ParentDatasetInfo toBuf(ParentDatasetInfo parentDatasetInfo) {
    if (parentDatasetInfo == null) {
      return null;
    }
    return JobsProtoUtil.toBuf(JobProtobuf.ParentDatasetInfo.getDefaultInstance().getParserForType(), parentDatasetInfo);
  }

  static NodeEndpoint toStuff(CoordinationProtos.NodeEndpoint endpoint) {
    return JobsProtoUtil.toStuff(NodeEndpoint.getDefaultInstance().cachedSchema(), endpoint);
  }

  static CoordinationProtos.NodeEndpoint toBuf(NodeEndpoint endpoint) {
    return JobsProtoUtil.toBuf(CoordinationProtos.NodeEndpoint.getDefaultInstance().getParserForType(), endpoint);
  }

  public static JobProtobuf.MaterializationSummary toBuf(MaterializationSummary materializationSummary) {
    return toBuf(JobProtobuf.MaterializationSummary.getDefaultInstance().getParserForType(), materializationSummary);
  }

  public static com.dremio.service.job.JobState toBuf(JobState jobState) {

    if (jobState == null) {
      return com.dremio.service.job.JobState.INVALID_JOB_STATE;
    }
    switch (jobState) {
      case NOT_SUBMITTED:
        return com.dremio.service.job.JobState.NOT_SUBMITTED;
      case STARTING:
        return com.dremio.service.job.JobState.STARTING;
      case PLANNING:
        return com.dremio.service.job.JobState.PLANNING;
      case RUNNING:
        return com.dremio.service.job.JobState.RUNNING;
      case COMPLETED:
        return com.dremio.service.job.JobState.COMPLETED;
      case CANCELED:
        return com.dremio.service.job.JobState.CANCELED;
      case FAILED:
        return com.dremio.service.job.JobState.FAILED;
      case CANCELLATION_REQUESTED:
        return com.dremio.service.job.JobState.CANCELLATION_REQUESTED;
      case ENQUEUED:
        return com.dremio.service.job.JobState.ENQUEUED;
      case PENDING:
        return com.dremio.service.job.JobState.PENDING;
      case METADATA_RETRIEVAL:
        return com.dremio.service.job.JobState.METADATA_RETRIEVAL;
      case QUEUED:
        return com.dremio.service.job.JobState.QUEUED;
      case ENGINE_START:
        return com.dremio.service.job.JobState.ENGINE_START;
      case EXECUTION_PLANNING:
        return com.dremio.service.job.JobState.EXECUTION_PLANNING;
      default:
        return com.dremio.service.job.JobState.INVALID_JOB_STATE;
    }
  }

  public static com.dremio.service.job.AttemptEvent.State toBuf(com.dremio.exec.proto.beans.AttemptEvent.State attemptState) {

    if (attemptState == null) {
      return com.dremio.service.job.AttemptEvent.State.INVALID_STATE;
    }
    switch (attemptState) {
      case METADATA_RETRIEVAL:
        return com.dremio.service.job.AttemptEvent.State.METADATA_RETRIEVAL;
      case STARTING:
        return com.dremio.service.job.AttemptEvent.State.STARTING;
      case PLANNING:
        return com.dremio.service.job.AttemptEvent.State.PLANNING;
      case RUNNING:
        return com.dremio.service.job.AttemptEvent.State.RUNNING;
      case COMPLETED:
        return com.dremio.service.job.AttemptEvent.State.COMPLETED;
      case CANCELED:
        return com.dremio.service.job.AttemptEvent.State.CANCELED;
      case FAILED:
        return com.dremio.service.job.AttemptEvent.State.FAILED;
      case QUEUED:
        return com.dremio.service.job.AttemptEvent.State.QUEUED;
      case PENDING:
        return com.dremio.service.job.AttemptEvent.State.PENDING;
      case ENGINE_START:
        return com.dremio.service.job.AttemptEvent.State.ENGINE_START;
      case EXECUTION_PLANNING:
        return com.dremio.service.job.AttemptEvent.State.EXECUTION_PLANNING;
      default:
        return com.dremio.service.job.AttemptEvent.State.INVALID_STATE;
    }
  }

  public static com.dremio.exec.proto.UserBitShared.AttemptEvent.State toBuf2(com.dremio.exec.proto.beans.AttemptEvent.State attemptState) {

    if (attemptState == null) {
      return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.INVALID_STATE;
    }
    switch (attemptState) {
      case METADATA_RETRIEVAL:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.METADATA_RETRIEVAL;
      case STARTING:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.STARTING;
      case PLANNING:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.PLANNING;
      case RUNNING:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.RUNNING;
      case COMPLETED:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.COMPLETED;
      case CANCELED:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.CANCELED;
      case FAILED:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.FAILED;
      case QUEUED:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.QUEUED;
      case PENDING:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.PENDING;
      case ENGINE_START:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.ENGINE_START;
      case EXECUTION_PLANNING:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.EXECUTION_PLANNING;
      default:
        return com.dremio.exec.proto.UserBitShared.AttemptEvent.State.INVALID_STATE;
    }
  }

  public static com.dremio.exec.proto.beans.AttemptEvent.State toBuf(com.dremio.service.job.AttemptEvent.State attemptState) {

    if (attemptState == null) {
      return com.dremio.exec.proto.beans.AttemptEvent.State.INVALID_STATE;
    }
    switch (attemptState) {
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

  public static com.dremio.exec.proto.beans.AttemptEvent.State toBuf(com.dremio.exec.proto.UserBitShared.AttemptEvent.State attemptState) {

    if (attemptState == null) {
      return com.dremio.exec.proto.beans.AttemptEvent.State.INVALID_STATE;
    }
    switch (attemptState) {
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

  public static List<AttemptEvent> toStuff2(List<com.dremio.exec.proto.beans.AttemptEvent> stateList) {
    List<AttemptEvent> res = null;
    if (stateList != null) {
      res = new ArrayList<>();
      List<AttemptEvent> finalRes = res;
      stateList.forEach(m -> finalRes
        .add(AttemptEvent.newBuilder()
          .setState(toBuf2(m.getState()))
          .setStartTime(m.getStartTime())
          .build()));
    }
    return res;
  }

  static List<com.dremio.exec.proto.beans.AttemptEvent> toBuf2(List<com.dremio.exec.proto.UserBitShared.AttemptEvent> stateList) {
    List<com.dremio.exec.proto.beans.AttemptEvent> res = null;
    if (stateList != null) {
      res = new ArrayList<>();
      for (AttemptEvent mS : stateList) {
        com.dremio.exec.proto.beans.AttemptEvent state = new com.dremio.exec.proto.beans.AttemptEvent();
        state.setState(toBuf(mS.getState()));
        state.setStartTime(mS.getStartTime());
        res.add(state);
      }
    }
    return res;
  }

  /**
   * Utility method that maps protobuf (proto3) jobstate to protostuf (proto2) jobstate
   */
  public static JobState toStuff(com.dremio.service.job.JobState jobState) {

    switch (jobState) {
      case NOT_SUBMITTED:
        return JobState.NOT_SUBMITTED;
      case STARTING:
        return JobState.STARTING;
      case PLANNING:
        return JobState.PLANNING;
      case RUNNING:
        return JobState.RUNNING;
      case COMPLETED:
        return JobState.COMPLETED;
      case CANCELED:
        return JobState.CANCELED;
      case FAILED:
        return JobState.FAILED;
      case CANCELLATION_REQUESTED:
        return JobState.CANCELLATION_REQUESTED;
      case ENQUEUED:
        return JobState.ENQUEUED;
      case PENDING:
        return JobState.PENDING;
      case METADATA_RETRIEVAL:
        return JobState.METADATA_RETRIEVAL;
      case QUEUED:
        return JobState.QUEUED;
      case ENGINE_START:
        return JobState.ENGINE_START;
      case EXECUTION_PLANNING:
        return JobState.EXECUTION_PLANNING;
      default:
        return null;
    }
  }

  /**
   * Utility method that maps protostuf (proto2) queryType to protobuf (proto3) queryType
   */
  public static com.dremio.service.job.QueryType toBuf(com.dremio.service.job.proto.QueryType queryType) {

    if (queryType == null) {
      return QueryType.UNKNOWN; // TODO: change if UNKNOWN has special meaning ?
    }
    switch (queryType) {
      case UI_RUN:
        return com.dremio.service.job.QueryType.UI_RUN;
      case UI_PREVIEW:
        return com.dremio.service.job.QueryType.UI_PREVIEW;
      case UI_INTERNAL_PREVIEW:
        return com.dremio.service.job.QueryType.UI_INTERNAL_PREVIEW;
      case UI_INTERNAL_RUN:
        return com.dremio.service.job.QueryType.UI_INTERNAL_RUN;
      case UI_EXPORT:
        return com.dremio.service.job.QueryType.UI_EXPORT;
      case ODBC:
        return com.dremio.service.job.QueryType.ODBC;
      case JDBC:
        return com.dremio.service.job.QueryType.JDBC;
      case REST:
        return com.dremio.service.job.QueryType.REST;
      case ACCELERATOR_CREATE:
        return com.dremio.service.job.QueryType.ACCELERATOR_CREATE;
      case ACCELERATOR_DROP:
        return com.dremio.service.job.QueryType.ACCELERATOR_DROP;
      case PREPARE_INTERNAL:
        return com.dremio.service.job.QueryType.PREPARE_INTERNAL;
      case ACCELERATOR_EXPLAIN:
        return com.dremio.service.job.QueryType.ACCELERATOR_EXPLAIN;
      case UI_INITIAL_PREVIEW:
        return com.dremio.service.job.QueryType.UI_INITIAL_PREVIEW;
      case FLIGHT:
        return com.dremio.service.job.QueryType.FLIGHT;
      default:
        return QueryType.UNKNOWN;
    }
  }

  /**
   * Utility method that maps protostuf (proto2) requestType to protobuf (proto3) requestType
   */
  public static com.dremio.service.job.RequestType toBuf(RequestType requestType) {

    if (requestType == null) {
      return com.dremio.service.job.RequestType.INVALID_REQUEST_TYPE;
    }
    switch (requestType) {
      case GET_CATALOGS:
        return com.dremio.service.job.RequestType.GET_CATALOGS;
      case GET_COLUMNS:
        return com.dremio.service.job.RequestType.GET_COLUMNS;
      case GET_SCHEMAS:
        return com.dremio.service.job.RequestType.GET_SCHEMAS;
      case GET_TABLES:
        return com.dremio.service.job.RequestType.GET_TABLES;
      case CREATE_PREPARE:
        return com.dremio.service.job.RequestType.CREATE_PREPARE;
      case EXECUTE_PREPARE:
        return com.dremio.service.job.RequestType.EXECUTE_PREPARE;
      case RUN_SQL:
        return com.dremio.service.job.RequestType.RUN_SQL;
      case GET_SERVER_META:
        return com.dremio.service.job.RequestType.GET_SERVER_META;
      default:
        return com.dremio.service.job.RequestType.INVALID_REQUEST_TYPE;
    }
  }

  public static RequestType toStuff(com.dremio.service.job.RequestType requestType) {
    if (requestType == com.dremio.service.job.RequestType.INVALID_REQUEST_TYPE) {
      return null;
    }
    switch (requestType) {
      case GET_CATALOGS:
        return RequestType.GET_CATALOGS;
      case GET_COLUMNS:
        return RequestType.GET_COLUMNS;
      case GET_SCHEMAS:
        return RequestType.GET_SCHEMAS;
      case GET_TABLES:
        return RequestType.GET_TABLES;
      case CREATE_PREPARE:
        return RequestType.CREATE_PREPARE;
      case RUN_SQL:
        return RequestType.RUN_SQL;
      case EXECUTE_PREPARE:
        return RequestType.EXECUTE_PREPARE;
      case GET_SERVER_META:
        return RequestType.GET_SERVER_META;
      default:
        return null;
    }
  }

  public static DatasetType toStuff(DatasetCommonProtobuf.DatasetType datasetType) {


    switch (datasetType) {
      case VIRTUAL_DATASET:
        return DatasetType.VIRTUAL_DATASET;
      case PHYSICAL_DATASET:
        return DatasetType.PHYSICAL_DATASET;
      case PHYSICAL_DATASET_SOURCE_FILE:
        return DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        return DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
      case PHYSICAL_DATASET_HOME_FILE:
        return DatasetType.PHYSICAL_DATASET_HOME_FILE;
      case PHYSICAL_DATASET_HOME_FOLDER:
        return DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
      default:
        return null;
    }
  }

  public static SearchTypes.SortOrder toStoreSortOrder(SearchJobsRequest.SortOrder order) {
    switch (order) {
      case ASCENDING:
        return SearchTypes.SortOrder.ASCENDING;
      case DESCENDING:
        return SearchTypes.SortOrder.DESCENDING;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static com.dremio.exec.work.user.SubstitutionSettings toPojo(SubstitutionSettings substitutionSettings) {
    final com.dremio.exec.work.user.SubstitutionSettings substitutionSettings1 = new com.dremio.exec.work.user.SubstitutionSettings(substitutionSettings.getExclusionsList());
    substitutionSettings1.setInclusions(substitutionSettings.getInclusionsList());
    return substitutionSettings1;
  }

  public static SubstitutionSettings toBuf(com.dremio.exec.work.user.SubstitutionSettings substitutionSettings) {
    final com.dremio.service.job.SubstitutionSettings.Builder substitutionSettingsOrBuilder = com.dremio.service.job.SubstitutionSettings.newBuilder();
    if (substitutionSettings.getExclusions() != null) {
      substitutionSettingsOrBuilder.addAllExclusions(substitutionSettings.getExclusions());
    }
    if (substitutionSettings.getInclusions() != null) {
      substitutionSettingsOrBuilder.addAllInclusions(substitutionSettings.getInclusions());
    }
    return substitutionSettingsOrBuilder.build();
  }

  public static com.dremio.service.job.proto.QueryType toStuff(QueryType queryType) {
    switch (queryType) {
      case UI_INITIAL_PREVIEW:
        return com.dremio.service.job.proto.QueryType.UI_INITIAL_PREVIEW;
      case UI_INTERNAL_PREVIEW:
        return com.dremio.service.job.proto.QueryType.UI_INTERNAL_PREVIEW;
      case UI_PREVIEW:
        return com.dremio.service.job.proto.QueryType.UI_PREVIEW;
      case PREPARE_INTERNAL:
        return com.dremio.service.job.proto.QueryType.PREPARE_INTERNAL;
      case UI_INTERNAL_RUN:
        return com.dremio.service.job.proto.QueryType.UI_INTERNAL_RUN;
      case UI_RUN:
        return com.dremio.service.job.proto.QueryType.UI_RUN;
      case UI_EXPORT:
        return com.dremio.service.job.proto.QueryType.UI_EXPORT;
      case ACCELERATOR_CREATE:
        return com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE;
      case ACCELERATOR_DROP:
        return com.dremio.service.job.proto.QueryType.ACCELERATOR_DROP;
      case ACCELERATOR_EXPLAIN:
        return com.dremio.service.job.proto.QueryType.ACCELERATOR_EXPLAIN;
      case ODBC:
        return com.dremio.service.job.proto.QueryType.ODBC;
      case JDBC:
        return com.dremio.service.job.proto.QueryType.JDBC;
      case REST:
        return com.dremio.service.job.proto.QueryType.REST;
      case FLIGHT:
        return com.dremio.service.job.proto.QueryType.FLIGHT;
      default:
        return com.dremio.service.job.proto.QueryType.UNKNOWN;
    }
  }

  public static MaterializationSummary toStuff(JobProtobuf.MaterializationSummary materializationSummary) {
    return toStuff(MaterializationSummary.getSchema(), materializationSummary);
  }

  public static SqlQuery toBuf(com.dremio.service.jobs.SqlQuery sqlQuery) {
    SqlQuery.Builder sqlQueryBuilder = SqlQuery.newBuilder();
    if (!Strings.isNullOrEmpty(sqlQuery.getSql())) {
      sqlQueryBuilder.setSql(sqlQuery.getSql());
    }
    if (sqlQuery.getContext() != null) {
      sqlQueryBuilder.addAllContext(sqlQuery.getContext());
    }
    if (!Strings.isNullOrEmpty(sqlQuery.getUsername())) {
      sqlQueryBuilder.setUsername(sqlQuery.getUsername());
    }
    if (!Strings.isNullOrEmpty(sqlQuery.getEngineName())) {
      sqlQueryBuilder.setEngineName(sqlQuery.getEngineName());
    }

    return sqlQueryBuilder.build();
  }

}
