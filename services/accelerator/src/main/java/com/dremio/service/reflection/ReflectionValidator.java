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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.apache.calcite.sql.type.SqlTypeFamily;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Validation logic for reflections
 */
public class ReflectionValidator {

  private final Provider<NamespaceService> namespaceService;
  private final Provider<CatalogService> catalogService;

  public ReflectionValidator(Provider<NamespaceService> namespaceService, Provider<CatalogService> catalogService) {
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
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
    DatasetConfig dataset = namespaceService.get().findDatasetByUUID(goal.getDatasetId());
    Preconditions.checkNotNull(dataset, "datasetId must reference an existing dataset");

    Catalog catalog = catalogService.get().getCatalog(SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build());
    DremioTable table = catalog.getTable(new NamespaceKey(dataset.getFullPathList()));
    List<ViewFieldType> schemaFields = ViewFieldsHelper.getBatchSchemaFields(table.getSchema());

    Map<String, ViewFieldType> schemaMap = Maps.newHashMap();
    for (ViewFieldType type : schemaFields) {
      schemaMap.put(type.getName(), type);
    }

    // Array, List, Map, and Union types cannot be used as dimension or measure field. They also cannot be used for sorting, partitioning or distribution in raw reflections.
    validateReflectionFieldsAgainstSchema(goal.getDetails().getDisplayFieldList(), schemaMap, "Display", false);
    validateReflectionFieldsAgainstSchema(goal.getDetails().getMeasureFieldList(), schemaMap, "Measure", true);
    validateReflectionFieldsAgainstSchema(goal.getDetails().getDistributionFieldList(), schemaMap, "Distribution", true);
    validateReflectionFieldsAgainstSchema(goal.getDetails().getSortFieldList(), schemaMap, "Sort", true);
    validateReflectionFieldsAgainstSchema(goal.getDetails().getPartitionFieldList(), schemaMap, "Partition", true);
    validateReflectionDimensionFieldsAgainstSchema(goal.getDetails().getDimensionFieldList(), schemaMap);
  }

  private void validateReflectionFieldsAgainstSchema(List<ReflectionField> fieldList, Map<String, ViewFieldType> schemaMap, String fieldName, Boolean mustBePrimitive) {
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

  private void validateReflectionDimensionFieldsAgainstSchema(List<ReflectionDimensionField> fieldList, Map<String, ViewFieldType> schemaMap) {
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
