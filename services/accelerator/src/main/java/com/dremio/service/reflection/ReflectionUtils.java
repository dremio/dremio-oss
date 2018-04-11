/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.JoinDependencyProperties;
import com.dremio.exec.planner.sql.ExternalMaterializationDescriptor;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.MaterializationDescriptor.ReflectionInfo;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
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
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

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

  public static Integer computeDatasetHash(DatasetConfig dataset, NamespaceService namespaceService) throws NamespaceException {
    Queue<DatasetConfig> q = new LinkedList<>();
    q.add(dataset);
    int hash = 1;
    while (!q.isEmpty()) {
      dataset = q.poll();
      if (isPhysicalDataset(dataset.getType())) {
        hash = 31 * hash + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
      } else {
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode();
        for (ParentDataset parent : dataset.getVirtualDataset().getParentsList()) {
          q.add(namespaceService.getDataset(new NamespaceKey(parent.getDatasetPathList())));
        }
      }
    }
    return hash;
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
      materialization.getVersion(),
      materialization.getExpiration(),
      materialization.getLogicalPlan().toByteArray(),
      getMaterializationPath(materialization),
      originalCost,
      materialization.getInitRefreshSubmit(),
      getPartitionNames(materialization.getPartitionList()),
      updateSettings,
      JoinDependencyProperties.NONE);
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

    if (!externalReflection.getQueryDatasetHash().equals(computeDatasetHash(queryDataset, namespaceService))) {
      logger.debug("Reflection {} excluded because query dataset {} is out of sync",
        externalReflection.getName(),
        PathUtils.constructFullPath(queryDataset.getFullPathList())
      );
      return null;
    }

    if (!externalReflection.getTargetDatasetHash().equals(computeDatasetHash(targetDataset, namespaceService))) {
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
        null,
        null,
        null,
        null,
        null,
        null
      ),
      externalReflection.getId(),
      Optional.fromNullable(externalReflection.getVersion()).or(Long.valueOf(0)),
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
      reflectionGoal.getName(),
      getNames(Optional.fromNullable(details.getSortFieldList()).or(Collections.<ReflectionField>emptyList())),
      getNames(Optional.fromNullable(details.getPartitionFieldList()).or(Collections.<ReflectionField>emptyList())),
      getNames(Optional.fromNullable(details.getDistributionFieldList()).or(Collections.<ReflectionField>emptyList())),
      getDimensionNames(Optional.fromNullable(details.getDimensionFieldList()).or(Collections.<ReflectionDimensionField>emptyList())),
      getNames(Optional.fromNullable(details.getMeasureFieldList()).or(Collections.<ReflectionField>emptyList())),
      getNames(Optional.fromNullable(details.getDisplayFieldList()).or(Collections.<ReflectionField>emptyList()))
    );
  }

  private static List<String> getNames(List<ReflectionField> fields){
    return FluentIterable.from(fields)
      .transform(new Function<ReflectionField, String>(){
        @Override
        public String apply(ReflectionField field) {
          return field.getName();
        }
      }).toList();
  }

  private static List<String> getDimensionNames(List<ReflectionDimensionField> fields){
    return FluentIterable.from(fields)
      .transform(new Function<ReflectionDimensionField, String>(){
        @Override
        public String apply(ReflectionDimensionField field) {
          return field.getName();
        }
      }).toList();
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

  public static List<ViewFieldType> removeUpdateColumn(final List<ViewFieldType> fields) {
    return FluentIterable.from(fields).filter(new Predicate<ViewFieldType>() {
      @Override
      public boolean apply(ViewFieldType input) {
        return !UPDATE_COLUMN.equals(input.getName());
      }
    }).toList();
  }

}
