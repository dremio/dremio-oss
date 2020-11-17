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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioCollectors;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

/***
 * Handles logic related to writer options
 */
public class WriterOptionManager {
  public static final Logger logger = LoggerFactory.getLogger(WriterOptionManager.class);
  public static final WriterOptionManager Instance = new WriterOptionManager();

  public WriterOptions buildWriterOptionForReflectionGoal(
    Integer ringCount,
    ReflectionGoal goal,
    List<String> availableFields
  ) {
    return buildWriterOptionForReflectionGoal(ringCount, goal, availableFields, false, true, null);
  }

  public WriterOptions buildWriterOptionForReflectionGoal(
    Integer ringCount,
    ReflectionGoal goal,
    List<String> availableFields,
    boolean isIcebergDataset,
    boolean isCreate,
    ByteString extendedByteString
  ) {
    ReflectionDetails details = goal.getDetails();

    PartitionDistributionStrategy dist;
    switch(details.getPartitionDistributionStrategy()) {
      case STRIPED:
        dist = PartitionDistributionStrategy.STRIPED;
        break;
      case CONSOLIDATED:
      default:
        dist = PartitionDistributionStrategy.HASH;
    }

    Map<String, String> availableFieldsToName = availableFields.stream()
      .collect(DremioCollectors.uniqueGrouping(String::toLowerCase));

    // For Iceberg write, set CREATE or INSERT option.
    WriterOptions.IcebergWriterOperation icebergWriterOperation =
      WriterOptions.IcebergWriterOperation.NONE;
    if (isIcebergDataset) {
      icebergWriterOperation = isCreate ? WriterOptions.IcebergWriterOperation.CREATE : WriterOptions.IcebergWriterOperation.INSERT;
    }

    return new WriterOptions(
      ringCount,
      validateAndPluckNames(details.getPartitionFieldList(), availableFieldsToName),
      validateAndPluckNames(details.getSortFieldList(), availableFieldsToName),
      validateAndPluckNames(details.getDistributionFieldList(), availableFieldsToName),
      dist,
      false,
      Long.MAX_VALUE,
      icebergWriterOperation,
      extendedByteString
    );
  }

  @VisibleForTesting List<String> validateAndPluckNames(List<ReflectionField> fields, Map<String, String> knownFields){
    if(fields == null || fields.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> fieldList = ImmutableList.builder();
    for(ReflectionField f : fields) {
      String foundField = knownFields.getOrDefault(f.getName().toLowerCase(), null);
      if(foundField != null) {
        fieldList.add(foundField);
      } else {
        throw UserException.validationError().message("Unable to find field %s.", f).build(logger);
      }
    }
    return fieldList.build();
  }

}
