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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioCollectors;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions.TableFormatOperation;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Handles logic related to writer options
 */
public class WriterOptionManager {
  private static final Logger logger = LoggerFactory.getLogger(WriterOptionManager.class);
  public static final WriterOptionManager Instance = new WriterOptionManager();

  public WriterOptions buildWriterOptionForReflectionGoal(
      Integer ringCount,
      ReflectionGoal goal,
      List<String> availableFields,
      SnapshotDiffContext snapshotDiffContext) {
    return buildWriterOptionForReflectionGoal(
        ringCount, goal, availableFields, false, true, null, snapshotDiffContext, null);
  }

  public WriterOptions buildWriterOptionForReflectionGoal(
      Integer ringCount,
      ReflectionGoal goal,
      List<String> availableFields,
      boolean isIcebergDataset,
      boolean isCreate,
      ByteString extendedByteString,
      SnapshotDiffContext snapshotDiffContext,
      Long previousIcebergSnapshot) {
    ReflectionDetails details = goal.getDetails();

    PartitionDistributionStrategy dist;
    switch (details.getPartitionDistributionStrategy()) {
      case STRIPED:
        dist = PartitionDistributionStrategy.STRIPED;
        break;
      case CONSOLIDATED:
      default:
        dist = PartitionDistributionStrategy.HASH;
    }

    Map<String, String> availableFieldsToName =
        availableFields.stream().collect(DremioCollectors.uniqueGrouping(String::toLowerCase));

    // For Iceberg write, set CREATE or INSERT option.
    ImmutableTableFormatWriterOptions.Builder tableFormatOptionsBuilder =
        new ImmutableTableFormatWriterOptions.Builder();
    if (isIcebergDataset) {
      tableFormatOptionsBuilder.setOperation(
          determineTableFormatOperation(isCreate, snapshotDiffContext));
      // Set the snapshot id from the Iceberg table for Reflections. This Snapshot Id is used the
      // starting snapshot
      // id when it needs to make the DML related commits into the Iceberg table.
      if (previousIcebergSnapshot != null) {
        tableFormatOptionsBuilder.setSnapshotId(previousIcebergSnapshot);
      }
    }

    return new WriterOptions(
        ringCount,
        validateAndPluckNames(
            toStringListReflectionPartitionField(details.getPartitionFieldList()),
            availableFieldsToName),
        validateAndPluckNames(
            toStringListReflectionField(details.getSortFieldList()), availableFieldsToName),
        validateAndPluckNames(
            toStringListReflectionField(details.getDistributionFieldList()), availableFieldsToName),
        dist,
        null,
        false,
        Long.MAX_VALUE,
        tableFormatOptionsBuilder.build(),
        extendedByteString);
  }

  /**
   * Returns TableFormatOperation based on isCreate, and Incremental refresh type in
   * snapshotDiffContext
   *
   * @param isCreate is this create operation
   * @param snapshotDiffContext context, to check if we are using
   *     SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS or not
   * @return the correct TableFormatOperation
   */
  private TableFormatOperation determineTableFormatOperation(
      boolean isCreate, SnapshotDiffContext snapshotDiffContext) {
    if (isCreate) {
      return TableFormatOperation.CREATE;
    }
    if (snapshotDiffContext != null
        && snapshotDiffContext.getFilterApplyOptions()
            == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
      return TableFormatOperation.UPDATE;
    }
    return TableFormatOperation.INSERT;
  }

  public static List<String> toStringListReflectionField(List<ReflectionField> fields) {
    if (fields == null || fields.isEmpty()) {
      return ImmutableList.of();
    }
    return fields.stream().map(x -> x.getName()).collect(Collectors.toList());
  }

  public static List<String> toStringListReflectionPartitionField(
      List<ReflectionPartitionField> fields) {
    if (fields == null || fields.isEmpty()) {
      return ImmutableList.of();
    }
    return fields.stream().map(x -> x.getName()).collect(Collectors.toList());
  }

  @VisibleForTesting
  List<String> validateAndPluckNames(List<String> fields, Map<String, String> knownFields) {
    if (fields == null || fields.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> fieldList = ImmutableList.builder();
    for (String f : fields) {
      String foundField = knownFields.getOrDefault(f.toLowerCase(), null);
      if (foundField != null) {
        fieldList.add(foundField);
      } else {
        throw UserException.validationError()
            .message("Unable to find field ReflectionField{name=%s}.", f)
            .build(logger);
      }
    }
    return fieldList.build();
  }
}
