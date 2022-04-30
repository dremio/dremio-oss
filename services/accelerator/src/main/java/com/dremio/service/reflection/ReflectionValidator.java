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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.sql.handlers.direct.AccelCreateReflectionHandler;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.MeasureType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Validation logic for reflections
 */
public class ReflectionValidator {

  private static final Logger logger = LoggerFactory.getLogger(ReflectionValidator.class);

  private final Provider<CatalogService> catalogService;

  public ReflectionValidator(Provider<CatalogService> catalogService) {
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
  }

  /**
   * @return false if reflection goal is invalid
   */
  public boolean isValid(ReflectionGoal goal) {
    try {
      validate(goal);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * @throws UserException if reflection goal is invalid
   */
  public void validate(ReflectionGoal goal) {
    ReflectionUtils.validateReflectionGoalWithoutSchema(goal);

    // The dataset that the reflection refers to must exist.
    final EntityExplorer entityExplorer = catalogService.get()
        .getCatalog(MetadataRequestOptions.newBuilder()
            .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
            .setCheckValidity(false)
            .build());

    final DremioTable table = entityExplorer.getTable(goal.getDatasetId());
    Preconditions.checkNotNull(table, "datasetId must reference an existing dataset");
    final List<ViewFieldType> schemaFields = ViewFieldsHelper.getBatchSchemaFields(table.getSchema());

    Map<String, ViewFieldType> schemaMap = Maps.newHashMap();
    for (ViewFieldType type : schemaFields) {
      schemaMap.put(type.getName(), type);
    }

    // Array, List, Map, and Union types cannot be used as dimension or measure field. They also cannot be used for sorting, partitioning or distribution in raw reflections.
    validateFields(goal.getDetails().getDisplayFieldList(), schemaMap, "Display", false);
    validateMeasures(goal.getDetails().getMeasureFieldList(), schemaMap, "Measure");
    validateFields(goal.getDetails().getDistributionFieldList(), schemaMap, "Distribution", true);
    validateFields(goal.getDetails().getSortFieldList(), schemaMap, "Sort", true);
    validateFields(goal.getDetails().getPartitionFieldList(), schemaMap, "Partition", true);
    validateDimensions(goal.getDetails().getDimensionFieldList(), schemaMap);
  }

  private void validateMeasures(List<ReflectionMeasureField> fieldList, Map<String, ViewFieldType> schemaMap, String fieldName) {
    if (fieldList == null) {
      return;
    }

    for (ReflectionMeasureField measureField : fieldList) {
      // A field must exist in the schema
      Preconditions.checkArgument(schemaMap.containsKey(measureField.getName()), String.format("%s field contains a field name [%s] that does not exist in the dataset", fieldName, measureField.getName()));

      ViewFieldType fieldType = schemaMap.get(measureField.getName());
      // We let ANY type pass primarily because pre-1.5 datasets may have valid fields marked as ANY.
      Preconditions.checkArgument(!Arrays.asList(SqlTypeFamily.ARRAY.name(), SqlTypeFamily.MAP.name()).contains(fieldType.getTypeFamily()), String.format("%s field cannot have field [%s] of type list, map or union", fieldName, measureField.getName()));


      final SqlTypeFamily family;
      try {
        family = SqlTypeFamily.valueOf(fieldType.getTypeFamily());
      } catch (RuntimeException ex) {
        throw UserException.validationError().message("Field %s cannot be configured as measure due to unexpected type of %s." ).build(logger);
      }
      final List<MeasureType> measures = AccelerationUtils.selfOrEmpty(measureField.getMeasureTypeList())
        .stream()
        .map(ReflectionValidator::toSqlMeasureType)
        .collect(Collectors.toList());

      // validate field types
      AccelCreateReflectionHandler.validate(fieldName, family, measures);
    }
  }

  private static final MeasureType toSqlMeasureType(com.dremio.service.reflection.proto.MeasureType measure) {
    switch(measure) {
    case APPROX_COUNT_DISTINCT: return MeasureType.APPROX_COUNT_DISTINCT;
    case COUNT: return MeasureType.COUNT;
    case MAX: return MeasureType.MAX;
    case MIN: return MeasureType.MIN;
    case SUM: return MeasureType.SUM;
    case UNKNOWN:
    default:
      return MeasureType.UNKNOWN;
    }
  }

  private static final com.dremio.service.reflection.proto.MeasureType fromSqlMeasureType(MeasureType measure) {
    switch(measure) {
    case APPROX_COUNT_DISTINCT: return com.dremio.service.reflection.proto.MeasureType.APPROX_COUNT_DISTINCT;
    case COUNT: return com.dremio.service.reflection.proto.MeasureType.COUNT;
    case MAX: return com.dremio.service.reflection.proto.MeasureType.MAX;
    case MIN: return com.dremio.service.reflection.proto.MeasureType.MIN;
    case SUM: return com.dremio.service.reflection.proto.MeasureType.SUM;
    case UNKNOWN:
    default:
      return com.dremio.service.reflection.proto.MeasureType.UNKNOWN;
    }
  }

  public static final Set<com.dremio.service.reflection.proto.MeasureType> getValidMeasures(RelDataTypeFamily family){
    Set<MeasureType> types = AccelCreateReflectionHandler.getValidMeasures(family);
    if(types == null) {
      return ImmutableSet.of();
    }

    return types.stream().map(ReflectionValidator::fromSqlMeasureType).collect(Collectors.toSet());
  }

  public static final List<com.dremio.service.reflection.proto.MeasureType> getDefaultMeasures(String family){
    try {
      return getDefaultMeasures(SqlTypeFamily.valueOf(family));
    } catch (RuntimeException e) {
      return ImmutableList.of();
    }
  }

  public static final List<com.dremio.service.reflection.proto.MeasureType> getDefaultMeasures(RelDataTypeFamily family){
    List<MeasureType> types = AccelCreateReflectionHandler.getDefaultMeasures(family);
    if(types == null) {
      return ImmutableList.of();
    }

    return types.stream().map(ReflectionValidator::fromSqlMeasureType).collect(Collectors.toList());
  }



  private void validateFields(List<ReflectionField> fieldList, Map<String, ViewFieldType> schemaMap, String fieldName, Boolean mustBePrimitive) {
    if (fieldList == null) {
      return;
    }

    for (ReflectionField reflectionField : fieldList) {
      // A field must exist in the schema
      Preconditions.checkArgument(schemaMap.containsKey(reflectionField.getName()), String.format("%s field contains a field name [%s] that does not exist in the dataset", fieldName, reflectionField.getName()));

      if (mustBePrimitive) {
        ViewFieldType fieldType = schemaMap.get(reflectionField.getName());
        // We let ANY type pass primarily because pre-1.5 datasets may have valid fields marked as ANY.
        Preconditions.checkArgument(!Arrays.asList(SqlTypeFamily.ARRAY.name(), SqlTypeFamily.MAP.name()).contains(fieldType.getTypeFamily()), String.format("%s field cannot have field [%s] of type list, map or union", fieldName, reflectionField.getName()));
      }
    }
  }

  private void validateDimensions(List<ReflectionDimensionField> fieldList, Map<String, ViewFieldType> schemaMap) {
    if (fieldList == null) {
      return;
    }

    for (ReflectionDimensionField reflectionField : fieldList) {
      // A field must exist in the schema
      Preconditions.checkArgument(schemaMap.containsKey(reflectionField.getName()), String.format("dimension field contains a field [%s] that does exist in the dataset", reflectionField.getName()));

      ViewFieldType fieldType = schemaMap.get(reflectionField.getName());
      // We let ANY type pass primarily because pre-1.5 datasets may have valid fields marked as ANY.
      Preconditions.checkArgument(!Arrays.asList(SqlTypeFamily.ARRAY.name(), SqlTypeFamily.MAP.name()).contains(fieldType.getTypeFamily()), String.format("dimension field cannot have field [%s] of type list, map or union", reflectionField.getName()));
    }
  }

}
