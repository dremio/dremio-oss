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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_PARTITION_TRANSFORMS;
import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;
import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_REFLECTION_ICEBERG_TRANSFORMS;
import static com.dremio.service.accelerator.AccelerationUtils.selfOrEmpty;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.commons.collections4.CollectionUtils;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.planner.acceleration.ExternalMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.JoinDependencyProperties;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor.ReflectionInfo;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
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
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.JobDetails;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
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
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.proto.Transform;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Helper functions for Reflection management
 */
public class ReflectionUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionUtils.class);

  public static boolean isHomeDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET_HOME_FILE || t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  @WithSpan
  public static JobId submitRefreshJob(JobsService jobsService, CatalogService catalogService, ReflectionEntry entry,
                                       MaterializationId materializationId, String sql, QueryType queryType, JobStatusListener jobStatusListener) {

    final SqlQuery query = SqlQuery.newBuilder()
      .setSql(sql)
      .addAllContext(Collections.<String>emptyList())
      .setUsername(SYSTEM_USERNAME)
      .build();
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    DatasetConfig config = CatalogUtil.getDatasetConfig(catalog, entry.getDatasetId());

    NamespaceKey datasetPathList = new NamespaceKey(config.getFullPathList());
    JobProtobuf.MaterializationSummary materializationSummary = JobProtobuf.MaterializationSummary.newBuilder()
      .setDatasetId(entry.getDatasetId())
      .setReflectionId(entry.getId().getId())
      .setLayoutVersion(entry.getTag())
      .setMaterializationId(materializationId.getId())
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
        .setQueryType(JobsProtoUtil.toBuf(queryType))
        .setVersionedDataset(VersionedDatasetPath.newBuilder().addAllPath(datasetPathList.getPathComponents()).build())
        .build(),
      new MultiJobStatusListener(submittedListener, jobStatusListener))
      .getJobId();
    submittedListener.await();
    Span.current().setAttribute("dremio.reflectionmanager.jobId", jobId.getId());
    Span.current().setAttribute("dremio.reflectionmanager.reflection", getId(entry));
    Span.current().setAttribute("dremio.reflectionmanager.materialization", getId(entry.getId(), materializationId));
    Span.current().setAttribute("dremio.reflectionmanager.datasetId", entry.getDatasetId());
    Span.current().setAttribute("dremio.reflectionmanager.sql", sql);
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
  public static String getId(ReflectionId reflectionId) {
    return String.format("reflection %s", reflectionId.getId());
  }

  /**
   * computes a log-friendly external reflection id
   */
  public static String getId(ExternalReflection externalReflection) {
    return String.format("external reflection %s[%s]", externalReflection.getId(), externalReflection.getName());
  }

  /**
   * computes a log-friendly reflection id
   */
  public static String getId(ReflectionEntry entry) {
    return String.format("reflection %s[%s]", entry.getId().getId(), entry.getName());
  }

  /**
   * computes a log-friendly reflection id
   */
  public static String getId(ReflectionGoal goal) {
    return String.format("reflection %s[%s]", goal.getId().getId(), goal.getName());
  }

  /**
   * computes a log-friendly materialization id
   */
  public static String getId(Materialization m) {
    return String.format("materialization %s/%s", m.getReflectionId().getId(), m.getId().getId());
  }

  /**
   * computes a log-friendly materialization id
   */
  public static String getId(ReflectionId reflectionId, MaterializationId materializationId) {
    return String.format("materialization %s/%s", reflectionId.getId(), materializationId.getId());
  }

  /**
   * computes a log-friendly reflection and materialization id
   */
  public static String getId(ReflectionEntry entry, Materialization m) {
    return String.format("materialization %s/%s[%s]", entry.getId().getId(), m.getId().getId(), entry.getName());
  }

  /**
   * computes a log-friendly materialization id
   */
  public static String getId(MaterializationDescriptor desc) {
    return String.format("materialization %s/%s", desc.getLayoutId(), desc.getMaterializationId());
  }

  /**
   * Creates and returns MaterializationDescriptor
   */
  public static MaterializationDescriptor getMaterializationDescriptor(
      final ReflectionGoal reflectionGoal,
      final ReflectionEntry reflectionEntry,
      final Materialization materialization,
      double originalCost,
      final CatalogService catalogService) {
    final IncrementalUpdateSettings updateSettings = new IncrementalUpdateSettings(
      reflectionEntry.getRefreshMethod() == RefreshMethod.INCREMENTAL,
      reflectionEntry.getRefreshField(),
      reflectionEntry.getSnapshotBased());
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
      materialization.getStripVersion(),
      catalogService);
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

  public static MaterializationDescriptor getMaterializationDescriptor(final ExternalReflection externalReflection,
                                                                       final CatalogService catalogService) throws NamespaceException {
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    DatasetConfig  queryDatasetConfig = CatalogUtil.getDatasetConfig(catalog, externalReflection.getQueryDatasetId());
    DatasetConfig targetDatasetConfig = CatalogUtil.getDatasetConfig(catalog, externalReflection.getTargetDatasetId());

    if (queryDatasetConfig == null) {
      logger.debug("Dataset {} not found", externalReflection.getQueryDatasetId());
      return null;
    }

    if (targetDatasetConfig  == null) {
      logger.debug("Dataset {} not found", externalReflection.getQueryDatasetId());
      return null;
    }

    if (!DatasetHashUtils.hashEquals(externalReflection.getQueryDatasetHash(), queryDatasetConfig, catalogService)) {
      logger.debug("Reflection {} excluded because query dataset {} is out of sync",
        externalReflection.getName(),
        PathUtils.constructFullPath(queryDatasetConfig.getFullPathList())
      );
      return null;
    }

    if (!DatasetHashUtils.hashEquals(externalReflection.getTargetDatasetHash(), targetDatasetConfig, catalogService)) {
      logger.debug("Reflection {} excluded because target dataset {} is out of sync",
        externalReflection.getName(),
        PathUtils.constructFullPath(targetDatasetConfig.getFullPathList())
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
      Optional.ofNullable(externalReflection.getTag()).orElse("0"),
      queryDatasetConfig.getFullPathList(),
      targetDatasetConfig.getFullPathList(),
      catalogService
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


  public static void validateReflectionGoalWithoutSchema(ReflectionGoal goal, OptionManager optionManager) {
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

    List<ReflectionField> partitionFieldListWithoutPartitionTransforms = null;
    if(details.getPartitionFieldList() != null) {
      partitionFieldListWithoutPartitionTransforms = details.getPartitionFieldList()
        .stream()
        .map(partitionField -> new ReflectionField(partitionField.getName()))
        .collect(Collectors.toList());
    }

    if (goal.getType() == com.dremio.service.reflection.proto.ReflectionType.RAW) {
      // For a raw reflection, at least one display field must be defined.
      Preconditions.checkArgument(CollectionUtils.isNotEmpty(details.getDisplayFieldList()), "raw reflection must have at least one display field configured");

      // For a raw reflection, only display, partition, sort, and distribution fields allowed (no dimension/measure).
      Preconditions.checkArgument(CollectionUtils.isEmpty(details.getDimensionFieldList()), "raw reflection can not contain dimension fields");
      Preconditions.checkArgument(CollectionUtils.isEmpty(details.getMeasureFieldList()), "raw reflection can not contain measure fields");

      // For a raw reflection, a column must be in the display list to be used as a partition, sort or distribution field.
      List<ReflectionField> displayFieldList = details.getDisplayFieldList();

      List<ReflectionField> fieldList = partitionFieldListWithoutPartitionTransforms;
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
          if (CollectionUtils.isNotEmpty(partitionFieldListWithoutPartitionTransforms)) {
            Preconditions.checkArgument(!partitionFieldListWithoutPartitionTransforms.contains(field), String.format("field [%s] cannot be both a sort and a partition", field.getName()));
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

      List<ReflectionField> fieldList = partitionFieldListWithoutPartitionTransforms;
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
          if (partitionFieldListWithoutPartitionTransforms != null) {
            Preconditions.checkArgument(!partitionFieldListWithoutPartitionTransforms.contains(field), String.format("field [%s] cannot be both a sort and a partition", field.getName()));
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
          CollectionUtils.isNotEmpty(partitionFieldListWithoutPartitionTransforms) ||
          CollectionUtils.isNotEmpty(details.getSortFieldList()) ||
          CollectionUtils.isNotEmpty(details.getDistributionFieldList()),
        "must have at least one non-empty field list"
      );
    }

    validateTransform(details.getPartitionFieldList(), optionManager);
  }

  /**
   * Check if a reflection has an issue with partition transforms
   * Throws exception if any issues are found
   * @param reflectionPartitionFields list of ReflectionPartitionField in the reflection
   * @param optionManager used for checking if needed support keys are enabled
   */
  public static void validateTransform(final List<ReflectionPartitionField> reflectionPartitionFields, OptionManager optionManager) {
    if(reflectionPartitionFields == null) {
      return;
    }
    final Set<String> transformFieldNames = new HashSet<>();
    for (final ReflectionPartitionField reflectionPartitionField : reflectionPartitionFields) {
      final boolean hasValidFieldName = reflectionPartitionField.getName() != null && !reflectionPartitionField.getName().isEmpty();

      //validate the same partition field is not used multiple times
      if(hasValidFieldName){
        Preconditions.checkArgument(!transformFieldNames.contains(reflectionPartitionField.getName().toLowerCase()),
          String.format("[%s] field is used multiple times in partition transforms.", reflectionPartitionField.getName()));
        transformFieldNames.add(reflectionPartitionField.getName().toLowerCase());
      }

      //we need to do the remaining checks even if field name is missing
      //this is because it is possible to just send TransformName/TransformArgs in the rest API without a field name
      PartitionTransform.Type typeFromReflectionDetails = null;
      if(reflectionPartitionField.getTransform() != null) {
        Preconditions.checkArgument(hasValidFieldName, String.format("Partition transform is present, but the field name is missing."));
        if(reflectionPartitionField.getTransform().getType() == null){
          Preconditions.checkArgument(false, String.format("Partition transform is present, but it is missing a transform type."));
        }

        typeFromReflectionDetails = PartitionTransform.Type.lookup(reflectionPartitionField.getTransform().getType().toString());
        //if allowPartitionTransformInReflections is disabled, the transform should be identity or empty
        if(!PartitionTransform.Type.IDENTITY.equals(typeFromReflectionDetails)) {
          ReflectionUtils.validateNonIdentityTransformAllowed(optionManager, reflectionPartitionField.getTransform().getType().toString());
        }
        if(reflectionPartitionField.getTransform().getType().equals(Transform.Type.TRUNCATE)){
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getTruncateTransform() != null,
            String.format("Truncate transform type for field [%s] is present, but no truncateTransform containing length is provided.",
              reflectionPartitionField.getName()));
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getBucketTransform() == null,
            String.format("Truncate transform type for field [%s] is present, but bucketTransform for this field is present too. A field can have only one transform.",
              reflectionPartitionField.getName()));
        } else if(reflectionPartitionField.getTransform().getType().equals(Transform.Type.BUCKET)){
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getTruncateTransform() == null,
            String.format("Bucket transform type for field [%s] is present, but truncateTransform for this field is present too. A field can have only one transform.",
              reflectionPartitionField.getName()));
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getBucketTransform() != null,
            String.format("Bucket transform type for field [%s] is present, but no bucketTransform containing bucket count is provided.",
              reflectionPartitionField.getName()));
        } else {
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getTruncateTransform() == null,
            String.format("Truncate transform type for field [%s] is present, but it does not match the transform type used.",
              reflectionPartitionField.getName()));
          Preconditions.checkArgument(reflectionPartitionField.getTransform().getBucketTransform() == null,
            String.format("Bucket transform type for field [%s] is present, but it does not match the transform type used.",
              reflectionPartitionField.getName()));
        }
      }
    }
  }

  public static void validateNonIdentityTransformAllowed(OptionManager optionManager, String transformName) {
    if(!optionManager.getOption(ENABLE_ICEBERG)){
      throw new RuntimeException(String.format("[%s] partition transform is present, but Iceberg support is disabled.",transformName));
    }
    if(!optionManager.getOption(ENABLE_ICEBERG_PARTITION_TRANSFORMS)){
      throw new RuntimeException(String.format("[%s] partition transform is present, but Iceberg partition support is disabled.",transformName));
    }
    if(!optionManager.getOption(ENABLE_REFLECTION_ICEBERG_TRANSFORMS)){
      throw new RuntimeException(String.format("[%s] partition transform is present, but Iceberg partition support for reflections is disabled.",transformName));
    }
  }

  /**
   * Given a list of ReflectionPartitionField build the corresponding list of PartitionTransform
   * @param reflectionPartitionFields input ReflectionPartitionFields to build PartitionTransform for
   * @return List of PartitionTransform corresponding to the input
   */
  public static List<PartitionTransform> buildPartitionTransforms(final List<ReflectionPartitionField> reflectionPartitionFields) {
    final List<PartitionTransform> transforms = new ArrayList<>();
    if(reflectionPartitionFields == null){
      return transforms;
    }
    transforms.addAll(reflectionPartitionFields.stream()
      .map(x->ReflectionUtils.getPartitionTransform(x))
      .collect(Collectors.toList()));

    return transforms;
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
      final String path = PathUtils.relativePath(
        Path.of(Path.getContainerSpecificRelativePath(Path.of(text.toString()))),
        Path.of(Path.getContainerSpecificRelativePath(accelerationBasePath)));

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
    int fileDelta = 0;
    while (true) {
      try (final JobDataFragment data = JobDataClientUtils.getJobData(jobsService, allocator, jobId, offset, fetchSize)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          final long fileSize = (Long) data.extractValue(RecordWriter.FILESIZE_COLUMN, i);
          final OperationType operationType = OperationType.valueOf((Integer) data.extractValue(RecordWriter.OPERATION_TYPE_COLUMN, i));
          //When we have incremental refresh by partition it is possible that we delete some files and add some new files
          //We use the flip sign here to adjust the reflection size accordingly
          //We will add up the size of any added files, and subtract the size of any deleted files
          final int flipSign = operationType != OperationType.DELETE_DATAFILE ? 1: -1;
          footprint += fileSize * flipSign;
          fileSizes.add(fileSize);
          fileDelta += flipSign;
        }
        offset += data.getReturnedRowCount();
      }
    }

    long medianFileSize = 0;
    //prevent an IndexOutOfBoundsException if numFiles is 0, and we are trying to get the 0th file
    if(fileSizes.size() > 0){
      // alternative is to implement QuickSelect to compute the median in linear time
      Collections.sort(fileSizes);
      medianFileSize = fileSizes.get(fileSizes.size() / 2);
    }

    return new MaterializationMetrics()
      .setFootprint(footprint)
      .setOriginalCost(JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getOriginalCost())
      .setMedianFileSize(medianFileSize)
      .setNumFiles(fileDelta);
  }

  public static String getIcebergReflectionBasePath(List<String> refreshPath, boolean isIcebergRefresh) {
    if (!isIcebergRefresh) {
      return "";
    }
    Preconditions.checkState(refreshPath.size() >= 2, "Unexpected state");
    return refreshPath.get(refreshPath.size() - 1);
  }

  public static boolean areReflectionDetailsEqual(ReflectionDetails details1, ReflectionDetails details2) {
    boolean equal = false;

    if (
      areBothListsEqual(details1.getDimensionFieldList(), details2.getDimensionFieldList()) &&
        areBothListsEqual(details1.getDisplayFieldList(), details2.getDisplayFieldList()) &&
        areBothListsEqual(details1.getDistributionFieldList(), details2.getDistributionFieldList()) &&
        areBothListsEqual(details1.getMeasureFieldList(), details2.getMeasureFieldList()) &&
        areBothListsEqual(details1.getPartitionFieldList(), details2.getPartitionFieldList()) &&
        areBothListsEqualWithOrder(details1.getSortFieldList(), details2.getSortFieldList()) &&
        details1.getPartitionDistributionStrategy().equals(details2.getPartitionDistributionStrategy())
    ) {
      equal = true;
    }

    return equal;
  }

  private static boolean areBothListsEqual(Collection collection1, Collection collection2) {
    // CollectionUtils.isEqualCollection is not null safe
    if (collection1 == null || collection2 == null) {
      return CollectionUtils.isEmpty(collection1) && CollectionUtils.isEmpty(collection2);
    } else {
      return CollectionUtils.isEqualCollection(collection1, collection2);
    }
  }

  private static <T> boolean areBothListsEqualWithOrder(List<T> list1, List<T> list2) {
    if (list1 == null || list2 == null) {
      return CollectionUtils.isEmpty(list1) && CollectionUtils.isEmpty(list2);
    } else {
      return list1.equals(list2);
    }
  }

  public static VersionedDatasetId getVersionDatasetId(String datasetId) {
    if (!VersionedDatasetId.isVersioned(datasetId)) {
      return null;
    }
    VersionedDatasetId versionedDatasetId;
    try {
      versionedDatasetId = VersionedDatasetId.fromString(datasetId);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
    return versionedDatasetId;
  }

  public static Map<String, VersionContext> buildVersionContext(String datasetId) {
    Map<String, VersionContext> sourceMappings = new HashMap<>();
    VersionedDatasetId versionedDatasetId = getVersionDatasetId(datasetId);
    if (versionedDatasetId == null) {
      return sourceMappings;
    }
    String source = versionedDatasetId.getTableKey().get(0);
    if (versionedDatasetId.getVersionContext().getType() == TableVersionType.BRANCH) {
      sourceMappings.put(source, VersionContext.ofBranch(versionedDatasetId.getVersionContext().getValue().toString()));
    } else if (versionedDatasetId.getVersionContext().getType() == TableVersionType.TAG) {
      sourceMappings.put(source, VersionContext.ofTag(versionedDatasetId.getVersionContext().getValue().toString()));
    } else if (versionedDatasetId.getVersionContext().getType() == TableVersionType.COMMIT) {
      sourceMappings.put(source, VersionContext.ofCommit(versionedDatasetId.getVersionContext().getValue().toString()));
    }
    return sourceMappings;
  }

  /**
   * Given a reflectionPartitionField build a corresponding PartitionTransform
   * @param reflectionPartitionField the input reflectionPartitionField
   * @return the corresponding PartitionTransform
   */
  public static PartitionTransform getPartitionTransform(ReflectionPartitionField reflectionPartitionField){
    final String fieldName = reflectionPartitionField.getName();
    //if there are no transforms provided (e.g. old reflection before we added transforms)
    //we will use PartitionTransform.Type.IDENTITY
    PartitionTransform.Type type = PartitionTransform.Type.IDENTITY;
    final List<Object> arguments = new ArrayList<>();
    if(reflectionPartitionField.getTransform() != null){
      final PartitionTransform.Type typeFromReflectionDetails = PartitionTransform.Type.lookup(reflectionPartitionField.getTransform().getType().name());
      if(typeFromReflectionDetails != null){
        type = typeFromReflectionDetails;
        if(reflectionPartitionField.getTransform().getType() == Transform.Type.BUCKET
            && reflectionPartitionField.getTransform().getBucketTransform().getBucketCount() != null) {
          arguments.addAll(ImmutableList.of(reflectionPartitionField.getTransform().getBucketTransform().getBucketCount()));
        } else if(reflectionPartitionField.getTransform().getType() == Transform.Type.TRUNCATE
            && reflectionPartitionField.getTransform().getTruncateTransform().getTruncateLength() != null) {
          arguments.addAll(ImmutableList.of(reflectionPartitionField.getTransform().getTruncateTransform().getTruncateLength()));
        }
      }
    }
    return new PartitionTransform(fieldName, type, arguments);
  }
}
