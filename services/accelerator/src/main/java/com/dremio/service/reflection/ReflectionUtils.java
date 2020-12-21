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
package com.dremio.service.reflection;

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;
import static com.dremio.service.accelerator.AccelerationUtils.selfOrEmpty;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.acceleration.ExternalMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.JoinDependencyProperties;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor.ReflectionInfo;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.io.file.Path;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.job.MaterializationSettings;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.SubstitutionSettings;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.MultiJobStatusListener;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.JobDetails;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.MeasureType;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Helper functions for Reflection management
 */
public class ReflectionUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionUtils.class);

  /**
   * @return true if the dataset type is PHYSICAL_*
   */
  public static boolean isPhysicalDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  public static boolean isHomeDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET_HOME_FILE || t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  public static Integer computeDatasetHash(DatasetConfig dataset, NamespaceService namespaceService, boolean ignorePds) throws NamespaceException {
    Queue<DatasetConfig> q = new LinkedList<>();
    q.add(dataset);
    int hash = 1;
    boolean isFirst = true;
    while (!q.isEmpty()) {
      dataset = q.poll();
      if (isPhysicalDataset(dataset.getType())) {
        if (!ignorePds || isFirst) {
          hash = 31 * hash + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
        }
      } else {
        int schemaHash = 0;
        if (isFirst) {
          final List<ViewFieldType> types = new ArrayList<>();
          dataset.getVirtualDataset().getSqlFieldsList().forEach(type -> {
            if (type.getSerializedField() != null) {
              ViewFieldType newType = new ViewFieldType();
              ProtostuffIOUtil.mergeFrom(ProtostuffIOUtil.toByteArray(type, ViewFieldType.getSchema(), LinkedBuffer.allocate()), newType, ViewFieldType.getSchema());
              types.add(newType.setSerializedField(null));
            } else {
              types.add(type);
            }
          });
          schemaHash = types.hashCode();
        }
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode() + schemaHash;
          for (ParentDataset parent : dataset.getVirtualDataset().getParentsList()) {
            int size = parent.getDatasetPathList().size();
            if( !(size > 1 && parent.getDatasetPathList().get(size-1).equalsIgnoreCase("external_query"))) {
              q.add(namespaceService.getDataset(new NamespaceKey(parent.getDatasetPathList())));
            }
          }
      }
      isFirst = false;
    }
    return hash;
  }

  public static JobId submitRefreshJob(JobsService jobsService, NamespaceService namespaceService, ReflectionEntry entry,
      Materialization materialization, String sql, JobStatusListener jobStatusListener) {
    final SqlQuery query = SqlQuery.newBuilder()
      .setSql(sql)
      .addAllContext(Collections.<String>emptyList())
      .setUsername(SYSTEM_USERNAME)
      .build();
    NamespaceKey datasetPathList = new NamespaceKey(namespaceService.findDatasetByUUID(entry.getDatasetId()).getFullPathList());
    JobProtobuf.MaterializationSummary materializationSummary = JobProtobuf.MaterializationSummary.newBuilder()
      .setDatasetId(entry.getDatasetId())
      .setReflectionId(entry.getId().getId())
      .setLayoutVersion(entry.getTag())
      .setMaterializationId(materialization.getId().getId())
      .setReflectionName(entry.getName())
      .setReflectionType(entry.getType().toString())
      .build();

    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final JobId jobId = jobsService.submitJob(
      SubmitJobRequest.newBuilder()
        .setMaterializationSettings(MaterializationSettings.newBuilder()
          .setMaterializationSummary(materializationSummary)
          .setSubstitutionSettings(SubstitutionSettings.newBuilder().addAllExclusions(ImmutableList.of()).build())
          .build())
        .setSqlQuery(query)
        .setQueryType(JobsProtoUtil.toBuf(QueryType.ACCELERATOR_CREATE))
        .setVersionedDataset(VersionedDatasetPath.newBuilder().addAllPath(datasetPathList.getPathComponents()).build())
        .build(),
      new MultiJobStatusListener(submittedListener, jobStatusListener));
    submittedListener.await();
    return jobId;
  }

  public static List<String> getMaterializationPath(Materialization materialization) {
    return ImmutableList.of(
      ACCELERATOR_STORAGEPLUGIN_NAME,
      materialization.getReflectionId().getId(),
      materialization.getId().getId()
    );
  }

  public static boolean isTerminal(MaterializationState state) {
    return state == MaterializationState.DONE || state == MaterializationState.FAILED || state == MaterializationState.CANCELED;
  }

  /**
   * computes a log-friendly reflection id
   */
  public static String getId(ReflectionEntry entry) {
    if (Strings.isNullOrEmpty(entry.getName())) {
      return entry.getId().getId();
    }
    return String.format("%s (%s)", entry.getId().getId(), entry.getName());
  }

  /**
   * computes a log-friendly reflection id
   */
  public static String getId(ReflectionGoal goal) {
    if (Strings.isNullOrEmpty(goal.getName())) {
      return goal.getId().getId();
    }
    return String.format("%s (%s)", goal.getId().getId(), goal.getName());
  }

  /**
   * computes a log-friendly materialization id
   */
  public static String getId(Materialization m) {
    return String.format("%s/%s", m.getReflectionId().getId(), m.getId().getId());
  }

  /**
   * Creates and returns MaterializationDescriptor
   */
  public static MaterializationDescriptor getMaterializationDescriptor(
      final ReflectionGoal reflectionGoal,
      final ReflectionEntry reflectionEntry,
      final Materialization materialization,
      double originalCost) {
    final IncrementalUpdateSettings updateSettings = new IncrementalUpdateSettings(
      reflectionEntry.getRefreshMethod() == RefreshMethod.INCREMENTAL,
      reflectionEntry.getRefreshField());
    return new MaterializationDescriptor(
      toReflectionInfo(reflectionGoal),
      materialization.getId().getId(),
      materialization.getTag(),
      materialization.getExpiration(),
      materialization.getLogicalPlan().toByteArray(),
      getMaterializationPath(materialization),
      originalCost,
      materialization.getInitRefreshSubmit(),
      getPartitionNames(materialization.getPartitionList()),
      updateSettings,
      JoinDependencyProperties.NONE,
      materialization.getLogicalPlanStrippedHash(),
      materialization.getStripVersion());
  }

  public static List<String> getPartitionNames(List<DataPartition> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      return Collections.emptyList();
    }

    return FluentIterable.from(partitions)
      .transform(new Function<DataPartition, String>() {
        @Override
        public String apply(DataPartition partition) {
          return partition.getAddress();
        }
      }).toList();
  }

  static boolean hasMissingPartitions(List<DataPartition> partitions, Set<String> hosts) {
    final List<String> partitionNames = getPartitionNames(partitions);
    return !hosts.containsAll(partitionNames);
  }

  /**
   * check with ignorePds true and then also false, for backward compatibility
   */
  static boolean hashEquals(int hash, DatasetConfig dataset, NamespaceService ns) throws NamespaceException {
    return
      hash == computeDatasetHash(dataset, ns, true)
        ||
      hash == computeDatasetHash(dataset, ns, false);
  }

  public static MaterializationDescriptor getMaterializationDescriptor(final ExternalReflection externalReflection,
                                                                       final NamespaceService namespaceService) throws NamespaceException {
    DatasetConfig queryDataset = namespaceService.findDatasetByUUID(externalReflection.getQueryDatasetId());
    DatasetConfig targetDataset = namespaceService.findDatasetByUUID(externalReflection.getTargetDatasetId());

    if (queryDataset == null) {
      logger.debug("Dataset {} not found", externalReflection.getQueryDatasetId());
      return null;
    }

    if (targetDataset == null) {
      logger.debug("Dataset {} not found", externalReflection.getQueryDatasetId());
      return null;
    }

    if (!hashEquals(externalReflection.getQueryDatasetHash(), queryDataset, namespaceService)) {
      logger.debug("Reflection {} excluded because query dataset {} is out of sync",
        externalReflection.getName(),
        PathUtils.constructFullPath(queryDataset.getFullPathList())
      );
      return null;
    }

    if (!hashEquals(externalReflection.getTargetDatasetHash(), targetDataset, namespaceService)) {
      logger.debug("Reflection {} excluded because target dataset {} is out of sync",
        externalReflection.getName(),
        PathUtils.constructFullPath(targetDataset.getFullPathList())
      );
      return null;
    }

    return new ExternalMaterializationDescriptor(
      new ReflectionInfo(
        externalReflection.getId(),
        ReflectionType.EXTERNAL,
        externalReflection.getName(),
        false,
        null,
        null,
        null,
        null,
        null,
        null
      ),
      externalReflection.getId(),
      Optional.fromNullable(externalReflection.getTag()).or(String.valueOf(0)),
      queryDataset.getFullPathList(),
      targetDataset.getFullPathList()
    );
  }

  public static Iterable<ReflectionGoal> getAllReflections(ReflectionGoalsStore userStore) {
    return userStore.getAllNotDeleted();
  }

  public static Iterable<Materialization> getAllMaterializations(MaterializationStore store) {
    return store.getAllMaterializations();
  }

  public static MaterializationDescriptor.ReflectionInfo toReflectionInfo(ReflectionGoal reflectionGoal){
    String id = reflectionGoal.getId().getId();
    final ReflectionDetails details = reflectionGoal.getDetails();
    return new MaterializationDescriptor.ReflectionInfo(id,
        reflectionGoal.getType() == com.dremio.service.reflection.proto.ReflectionType.RAW ? ReflectionType.RAW : ReflectionType.AGG,
        reflectionGoal.getName(), reflectionGoal.getArrowCachingEnabled(),
        s(details.getSortFieldList()).map(t -> t.getName()).collect(Collectors.toList()),
        s(details.getPartitionFieldList()).map(t -> t.getName()).collect(Collectors.toList()),
        s(details.getDistributionFieldList()).map(t -> t.getName()).collect(Collectors.toList()),
        s(details.getDimensionFieldList()).map(t -> t.getName()).collect(Collectors.toList()),
        s(details.getMeasureFieldList()).map(ReflectionUtils::toMeasureColumn).collect(Collectors.toList()),
        s(details.getDisplayFieldList()).map(t -> t.getName()).collect(Collectors.toList())
    );
  }

  private static UserBitShared.MeasureColumn toMeasureColumn(ReflectionMeasureField field) {
    List<String> fields =  AccelerationUtils.selfOrEmpty(field.getMeasureTypeList())
      .stream()
      .map(MeasureType::toString)
      .collect(Collectors.toList());

    UserBitShared.MeasureColumn measureColumn = UserBitShared.MeasureColumn.newBuilder().setName(field.getName())
      .addAllMeasureType(fields).build();

    return measureColumn;
  }

  private static <T> Stream<T> s(Collection<T> collection){
    if(collection == null) {
      return Stream.of();
    }

    return collection.stream();
  }


  public static void validateReflectionGoalWithoutSchema(ReflectionGoal goal) {
    Preconditions.checkArgument(goal.getDatasetId() != null, "datasetId required");

    // Reflections must have a non-empty name
    Preconditions.checkNotNull(goal.getName(), "non-empty name required");
    Preconditions.checkArgument(!goal.getName().trim().isEmpty(), "non-empty name required");
    Preconditions.checkNotNull(goal.getType(), "type must be set");

    Preconditions.checkNotNull(goal.getState(), "must have state");

    // only validate enabled reflection goals
    if (goal.getState() != ReflectionGoalState.ENABLED) {
      return;
    }

    // validate details
    ReflectionDetails details = goal.getDetails();
    Preconditions.checkArgument(details != null, "reflection details are required");

    if (goal.getType() == com.dremio.service.reflection.proto.ReflectionType.RAW) {
      // For a raw reflection, at least one display field must be defined.
      Preconditions.checkArgument(CollectionUtils.isNotEmpty(details.getDisplayFieldList()), "raw reflection must have at least one display field configured");

      // For a raw reflection, only display, partition, sort, and distribution fields allowed (no dimension/measure).
      Preconditions.checkArgument(CollectionUtils.isEmpty(details.getDimensionFieldList()), "raw reflection can not contain dimension fields");
      Preconditions.checkArgument(CollectionUtils.isEmpty(details.getMeasureFieldList()), "raw reflection can not contain measure fields");

      // For a raw reflection, a column must be in the display list to be used as a partition, sort or distribution field.
      List<ReflectionField> displayFieldList = details.getDisplayFieldList();

      List<ReflectionField> fieldList = details.getPartitionFieldList();
      if (fieldList != null) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(displayFieldList != null && displayFieldList.contains(field), String.format("partition field [%s] must also be a display field", field.getName()));
        }
      }

      fieldList = details.getSortFieldList();
      if (CollectionUtils.isNotEmpty(fieldList)) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(displayFieldList != null && displayFieldList.contains(field), String.format("sort field [%s] must also be a display field", field.getName()));

          // A field cannot be both a sort and partition field.
          if (CollectionUtils.isNotEmpty(details.getPartitionFieldList())) {
            Preconditions.checkArgument(!details.getPartitionFieldList().contains(field), String.format("field [%s] cannot be both a sort and a partition", field.getName()));
          }
        }
      }

      fieldList = details.getDistributionFieldList();
      if (CollectionUtils.isNotEmpty(fieldList)) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(displayFieldList != null && displayFieldList.contains(field), String.format("distribution field [%s] must also be a display field", field.getName()));
        }
      }
    } else {
      // For a agg reflection, only dimension, measure, partition, sort, distribution fields allowed (no display).
      Preconditions.checkArgument(CollectionUtils.isEmpty(details.getDisplayFieldList()), "aggregate reflection can not contain display fields");

      // For a agg reflection, only dimension fields can be selected for sort, partition or distribution fields.
      List<ReflectionDimensionField> dimensionFieldList = details.getDimensionFieldList();

      List<ReflectionField> fieldList = details.getPartitionFieldList();
      if (CollectionUtils.isNotEmpty(fieldList)) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(dimensionFieldList != null && doesPartitionFieldListContainField(dimensionFieldList, field), String.format("partition field [%s] must also be a dimension field", field.getName()));
        }
      }

      fieldList = details.getSortFieldList();
      if (CollectionUtils.isNotEmpty(fieldList)) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(dimensionFieldList != null && doesPartitionFieldListContainField(dimensionFieldList, field), String.format("sort field [%s] must also be a dimension field", field.getName()));

          // A field cannot be both a sort and partition field.
          if (details.getPartitionFieldList() != null) {
            Preconditions.checkArgument(!details.getPartitionFieldList().contains(field), String.format("field [%s] cannot be both a sort and a partition", field.getName()));
          }
        }
      }

      fieldList = details.getDistributionFieldList();
      if (CollectionUtils.isNotEmpty(fieldList)) {
        for (ReflectionField field : fieldList) {
          Preconditions.checkArgument(dimensionFieldList != null && doesPartitionFieldListContainField(dimensionFieldList, field), String.format("distribution field [%s] must also be a dimension field", field.getName()));
        }
      }

      // A agg reflection must have at least one non-empty field list
      Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(details.getDimensionFieldList()) ||
          CollectionUtils.isNotEmpty(details.getMeasureFieldList()) ||
          CollectionUtils.isNotEmpty(details.getPartitionFieldList()) ||
          CollectionUtils.isNotEmpty(details.getSortFieldList()) ||
          CollectionUtils.isNotEmpty(details.getDistributionFieldList()),
        "must have at least one non-empty field list"
      );
    }
  }

  private static boolean doesPartitionFieldListContainField(List<ReflectionDimensionField> dimensionFieldList, ReflectionField field) {
    boolean contains = false;

    for (ReflectionDimensionField dimensionField : dimensionFieldList) {
      if (dimensionField.getName().equals(field.getName())) {
        contains = true;
        break;
      }
    }

    return contains;
  }

  /**
   * Removes an update column from a persisted column. This means it won't be written and won't be used in matching.
   *
   * If there is not initial update column, no changes will be made.
   */
  public static RelNode removeUpdateColumn(RelNode node){
    if(node.getTraitSet() == null){
      // for test purposes.
      return node;
    }
    RelDataTypeField field = node.getRowType().getField(UPDATE_COLUMN, false, false);
    if(field == null ){
      return node;
    }
    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    final RelDataTypeFactory.FieldInfoBuilder rowTypeBuilder = new RelDataTypeFactory.FieldInfoBuilder(node.getCluster().getTypeFactory());
    final List<RexNode> projects = FluentIterable.from(node.getRowType().getFieldList())
      .filter(new Predicate<RelDataTypeField>(){
        @Override
        public boolean apply(RelDataTypeField input) {
          return !UPDATE_COLUMN.equals(input.getName());
        }})
      .transform(new Function<RelDataTypeField, RexNode>(){
        @Override
        public RexNode apply(RelDataTypeField input) {
          rowTypeBuilder.add(input);
          return rexBuilder.makeInputRef(input.getType(), input.getIndex());
        }}).toList();

    return new LogicalProject(node.getCluster(), node.getTraitSet(), node, projects, rowTypeBuilder.build());
  }

  public static RelNode removeColumns(RelNode node, Predicate<RelDataTypeField> predicate) {
    if (node.getTraitSet() == null) {
      // for test purposes.
      return node;
    }

    // identify all fields that match passed predicate
    Set<RelDataTypeField> toRemove = FluentIterable.from(node.getRowType().getFieldList()).filter(predicate).toSet();

    if (toRemove.isEmpty()) {
      return node;
    }

    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    final RelDataTypeFactory.FieldInfoBuilder rowTypeBuilder = new RelDataTypeFactory.FieldInfoBuilder(node.getCluster().getTypeFactory());
    final List<RexNode> projects = FluentIterable.from(node.getRowType().getFieldList())
      .filter(Predicates.not(toRemove::contains))
      .transform((RelDataTypeField field) -> {
        rowTypeBuilder.add(field);
        return (RexNode) rexBuilder.makeInputRef(field.getType(), field.getIndex());
      }).toList();

    return new LogicalProject(node.getCluster(), node.getTraitSet(), node, projects, rowTypeBuilder.build());
  }

  public static List<ViewFieldType> removeUpdateColumn(final List<ViewFieldType> fields) {
    return FluentIterable.from(fields).filter(new Predicate<ViewFieldType>() {
      @Override
      public boolean apply(ViewFieldType input) {
        return !UPDATE_COLUMN.equals(input.getName());
      }
    }).toList();
  }

  public static Refresh createRefresh(ReflectionId reflectionId, List<String> refreshPath, final long seriesId, final int seriesOrdinal,
                                      final UpdateId updateId, JobDetails details, MaterializationMetrics metrics, List<DataPartition> dataPartitions,
                                      final boolean isIcebergRefresh, final String icebergBasePath) {
    final String path = PathUtils.getPathJoiner().join(Iterables.skip(refreshPath, 1));

    return new Refresh()
      .setId(new RefreshId(UUID.randomUUID().toString()))
      .setReflectionId(reflectionId)
      .setPartitionList(dataPartitions)
      .setMetrics(metrics)
      .setCreatedAt(System.currentTimeMillis())
      .setSeriesId(seriesId)
      .setUpdateId(updateId)
      .setSeriesOrdinal(seriesOrdinal)
      .setPath(path)
      .setJob(details)
      .setIsIcebergRefresh(isIcebergRefresh)
      .setBasePath(icebergBasePath);
  }

  public static List<String> getRefreshPath(final JobId jobId, final Path accelerationBasePath, JobsService jobsService, BufferAllocator allocator) {
    // extract written path from writer's metadata
    try (JobDataFragment data = JobDataClientUtils.getJobData(jobsService, allocator, jobId, 0, 1)) {
      Text text = (Text) Preconditions.checkNotNull(data.extractValue(RecordWriter.PATH_COLUMN, 0),
        "Empty write path for job %s", jobId.getId());

      // relative path to the acceleration base path
      final String path = PathUtils.relativePath(Path.of(text.toString()), accelerationBasePath);

      // extract first 2 components of the path "<reflection-id>."<modified-materialization-id>"
      List<String> components = PathUtils.toPathComponents(path);
      Preconditions.checkState(components.size() >= 2, "Refresh path %s is incomplete", path);

      return ImmutableList.of(ACCELERATOR_STORAGEPLUGIN_NAME, components.get(0), components.get(1));
    }
  }

  public static JobDetails computeJobDetails(final JobAttempt jobAttempt) {
    final JobInfo info = jobAttempt.getInfo();

    final JobDetails details = new JobDetails()
      .setJobId(info.getJobId().getId())
      .setJobStart(info.getStartTime())
      .setJobEnd(info.getFinishTime());

    final JobStats stats = jobAttempt.getStats();
    if (stats != null) {
      details
        .setInputBytes(stats.getInputBytes())
        .setInputRecords(stats.getInputRecords())
        .setOutputBytes(stats.getOutputBytes())
        .setOutputRecords(stats.getOutputRecords());
    }
    return details;
  }

  public static List<DataPartition> computeDataPartitions(JobInfo jobInfo) {
    return FluentIterable
      .from(selfOrEmpty(jobInfo.getPartitionsList()))
      .transform(DataPartition::new)
      .toList();
  }

  public static MaterializationMetrics computeMetrics(com.dremio.service.job.JobDetails jobDetails, JobsService jobsService, BufferAllocator allocator, JobId jobId) {
    final int fetchSize = 1000;

    // collect the size of all files
    final List<Long> fileSizes = Lists.newArrayList();
    int offset = 0;
    long footprint = 0;

    while (true) {
      try (final JobDataFragment data = JobDataClientUtils.getJobData(jobsService, allocator, jobId, offset, fetchSize)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          final long fileSize = (Long) data.extractValue(RecordWriter.FILESIZE_COLUMN, i);
          footprint += fileSize;
          fileSizes.add(fileSize);
        }
        offset += data.getReturnedRowCount();
      }
    }

    final int numFiles = fileSizes.size();
    // alternative is to implement QuickSelect to compute the median in linear time
    Collections.sort(fileSizes);
    final long medianFileSize = fileSizes.get(numFiles / 2);

    return new MaterializationMetrics()
      .setFootprint(footprint)
      .setOriginalCost(JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getOriginalCost())
      .setMedianFileSize(medianFileSize)
      .setNumFiles(numFiles);
  }

  public static String getIcebergReflectionBasePath(Materialization materialization, List<String> refreshPath, boolean isIcebergRefresh) {
    if (materialization.getBasePath() != null && !materialization.getBasePath().isEmpty()) {
      return materialization.getBasePath();
    }
    if (isIcebergRefresh) {
      Preconditions.checkState(refreshPath.size() >= 2, "Unexpected state");
      return refreshPath.get(refreshPath.size() - 1);
    }
    return "";
  }
}
