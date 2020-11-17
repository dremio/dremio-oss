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
package com.dremio.dac.util;

import static com.dremio.dac.proto.model.dataset.ExtractRuleType.pattern;
import static com.dremio.dac.proto.model.dataset.ExtractRuleType.position;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.util.MajorTypeHelper;
import com.dremio.dac.explore.DataTypeUtil;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.Field;
import com.dremio.dac.model.common.VisitorException;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.ExtractRulePattern;
import com.dremio.dac.proto.model.dataset.ExtractRulePosition;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.proto.model.dataset.IndexType;
import com.dremio.dac.proto.model.dataset.Offset;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * To deal with protostuff
 */
public class DatasetsUtil {

  /**
   * Helper method which gets the completed preview data job for given query on given dataset path and version.
   */
  public static JobData getDatasetPreviewJob(QueryExecutor executor, SqlQuery query, DatasetPath datasetPath, DatasetVersion version) {
    // In most cases we should already have a preview job that ran on the given dataset version. If not it will trigger a job.
    CompletionListener completionListener = new CompletionListener();
    JobData jobData = executor.runQueryWithListener(query, QueryType.UI_PREVIEW, datasetPath, version, completionListener);

    // Wait for the job to complete (will return immediately if the job already exists, otherwise a blocking call until
    // the job completes).
    completionListener.awaitUnchecked();

    return jobData;
  }

  /**
   * Visitor interface for {@link ExtractRule} instances
   *
   * @param <T> return type for visitor methods
   */
  public interface ExtractRuleVisitor<T> {

    T visit(ExtractRulePattern extractRulePattern) throws Exception;

    T visit(ExtractRulePosition position) throws Exception;

  }

  public static <T> T accept(ExtractRule extractRule, ExtractRuleVisitor<T> visitor) {
    try {
      switch (extractRule.getType()) {
      case pattern:
        return visitor.visit(extractRule.getPattern());
      case position:
        return visitor.visit(extractRule.getPosition());
      default:
        throw new UnsupportedOperationException("unknown type " + extractRule.getType());
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new VisitorException("visitor threw an exception visiting " + extractRule.getType(), e);
    }
  }

  public static ExtractRule position(Offset start, Offset end) {
    ExtractRule extractRule = new ExtractRule(position);
    extractRule.setPosition(new ExtractRulePosition(start, end));
    return extractRule;
  }

  public static ExtractRule pattern(String patternStr, int index, IndexType indexType) {
    ExtractRule extractRule = new ExtractRule(pattern);
    extractRule.setPattern(new ExtractRulePattern(patternStr, index, indexType));
    return extractRule;
  }

  public static VirtualDatasetVersion toVirtualDatasetVersion(VirtualDatasetUI virtualDatasetUI) {
    final VirtualDatasetVersion vvds = new VirtualDatasetVersion();
    final DatasetConfig datasetConfig = new DatasetConfig();
    final VirtualDataset virtualDataset = toVirtualDataset(virtualDatasetUI);

    vvds.setLastTransform(virtualDatasetUI.getLastTransform());
    vvds.setState(virtualDatasetUI.getState());
    vvds.setPreviousVersion(virtualDatasetUI.getPreviousVersion());
    vvds.setNamed(virtualDatasetUI.getIsNamed());
    vvds.setDerivation(virtualDatasetUI.getDerivation());

    datasetConfig.setName(virtualDatasetUI.getName());
    datasetConfig.setOwner(virtualDatasetUI.getOwner());
    datasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    datasetConfig.setCreatedAt(virtualDatasetUI.getCreatedAt());
    datasetConfig.setFullPathList(virtualDatasetUI.getFullPathList());
    datasetConfig.setTag(virtualDatasetUI.getSavedTag());
    datasetConfig.setVirtualDataset(virtualDataset);
    datasetConfig.setRecordSchema(virtualDatasetUI.getRecordSchema());
    if (virtualDatasetUI.getId() != null) {
      datasetConfig.setId(new EntityId(virtualDatasetUI.getId()));
    }

    vvds.setDataset(datasetConfig);
    return vvds;
  }

  public static VirtualDataset toVirtualDataset(VirtualDatasetUI virtualDatasetUI) {
    final VirtualDataset virtualDataset = new VirtualDataset();

    virtualDataset.setContextList(virtualDatasetUI.getState().getContextList());
    virtualDataset.setSql(virtualDatasetUI.getSql());
    virtualDataset.setParentsList(virtualDatasetUI.getParentsList());
    virtualDataset.setGrandParentsList(virtualDatasetUI.getGrandParentsList());
    virtualDataset.setVersion(virtualDatasetUI.getVersion());
    virtualDataset.setFieldOriginsList(virtualDatasetUI.getFieldOriginsList());
    virtualDataset.setSqlFieldsList(virtualDatasetUI.getSqlFieldsList());
    virtualDataset.setCalciteFieldsList(virtualDatasetUI.getCalciteFieldsList());

    return virtualDataset;
  }

  public static VirtualDatasetUI toVirtualDatasetUI(VirtualDatasetVersion vvds) {
    if (vvds == null) {
      return null;
    }
    final VirtualDatasetUI virtualDatasetUI = new VirtualDatasetUI();

    final DatasetConfig datasetConfig = vvds.getDataset();
    final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();

    virtualDatasetUI.setOwner(datasetConfig.getOwner());
    virtualDatasetUI.setName(datasetConfig.getName());
    virtualDatasetUI.setCreatedAt(datasetConfig.getCreatedAt());
    virtualDatasetUI.setFullPathList(datasetConfig.getFullPathList());
    virtualDatasetUI.setSavedTag(datasetConfig.getTag());
    if (datasetConfig.getId() != null) {
      virtualDatasetUI.setId(datasetConfig.getId().getId());
    }
    virtualDatasetUI.setIsNamed(vvds.getNamed());
    virtualDatasetUI.setSqlFieldsList(ViewFieldsHelper.getViewFields(datasetConfig));
    virtualDatasetUI.setCalciteFieldsList(ViewFieldsHelper.getCalciteViewFields(datasetConfig));
    virtualDatasetUI.setParentsList(virtualDataset.getParentsList());
    virtualDatasetUI.setGrandParentsList(virtualDataset.getGrandParentsList());
    virtualDatasetUI.setFieldOriginsList(virtualDataset.getFieldOriginsList());
    virtualDatasetUI.setVersion(virtualDataset.getVersion());

    virtualDatasetUI.setState(vvds.getState());
    virtualDatasetUI.setPreviousVersion(vvds.getPreviousVersion());
    virtualDatasetUI.setLastTransform(vvds.getLastTransform());

    virtualDatasetUI.setDerivation(vvds.getDerivation());
    virtualDatasetUI.setSql(virtualDataset.getSql());
    virtualDatasetUI.setContextList(virtualDataset.getContextList());
    virtualDatasetUI.setRecordSchema(datasetConfig.getRecordSchema());

    virtualDatasetUI.getState().setContextList(virtualDataset.getContextList());

    return virtualDatasetUI;
  }

  public static PhysicalDatasetConfig toPhysicalDatasetConfig(DatasetConfig datasetConfig) {
    checkNotNull(datasetConfig.getPhysicalDataset());
    final com.dremio.service.namespace.dataset.proto.PhysicalDataset physicalDataset = datasetConfig.getPhysicalDataset();
    final PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
    physicalDatasetConfig.setFormatSettings(physicalDataset.getFormatSettings());
    physicalDatasetConfig.setFullPathList(datasetConfig.getFullPathList());
    physicalDatasetConfig.setType(datasetConfig.getType());
    physicalDatasetConfig.setName(datasetConfig.getName());
    physicalDatasetConfig.setTag(datasetConfig.getTag());
    physicalDatasetConfig.setId(datasetConfig.getId().getId());
    return physicalDatasetConfig;
  }

  public static DatasetConfig toDatasetConfig(PhysicalDatasetConfig physicalDatasetConfig, String owner) {
    final DatasetConfig datasetConfig = new DatasetConfig();

    if (physicalDatasetConfig.getId() != null) {
      datasetConfig.setId(new EntityId(physicalDatasetConfig.getId()));
    }
    datasetConfig.setOwner(owner);
    datasetConfig.setFullPathList(physicalDatasetConfig.getFullPathList());
    datasetConfig.setName(physicalDatasetConfig.getName());
    datasetConfig.setType(physicalDatasetConfig.getType());
    datasetConfig.setTag(physicalDatasetConfig.getTag());
    datasetConfig.setPhysicalDataset(new com.dremio.service.namespace.dataset.proto.PhysicalDataset().setFormatSettings(
      physicalDatasetConfig.getFormatSettings()));
    return datasetConfig;
  }

  public static DatasetConfig toDatasetConfig(FileConfig fileConfig, DatasetType datasetType, String owner, EntityId id) {
    final DatasetConfig datasetConfig = new DatasetConfig();
    Objects.requireNonNull(id, "EntityId must be defined.");
    Objects.requireNonNull(id.getId(), "EntityId must be defined.");
    datasetConfig.setOwner(owner);
    datasetConfig.setFullPathList(fileConfig.getFullPathList());
    datasetConfig.setName(fileConfig.getName());
    datasetConfig.setOwner(fileConfig.getOwner());
    datasetConfig.setTag(fileConfig.getTag());
    datasetConfig.setType(datasetType);
    datasetConfig.setCreatedAt(fileConfig.getCtime());
    datasetConfig.setId(id);

    datasetConfig.setPhysicalDataset(new com.dremio.service.namespace.dataset.proto.PhysicalDataset().setFormatSettings(fileConfig));
    return datasetConfig;
  }

  public static FileConfig toFileConfig(DatasetConfig datasetConfig) {
    final FileConfig fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();

    fileConfig.setCtime(datasetConfig.getCreatedAt());
    fileConfig.setTag(datasetConfig.getTag());
    fileConfig.setOwner(datasetConfig.getOwner());
    fileConfig.setFullPathList(datasetConfig.getFullPathList());
    fileConfig.setName(datasetConfig.getName());

    return fileConfig;
  }

  public static VirtualDatasetUI getHeadVersion(DatasetPath datasetPath, NamespaceService namespaceService,
      DatasetVersionMutator datasetService) throws NamespaceException, DatasetVersionNotFoundException {
    DatasetConfig dsConfig = namespaceService.getDataset(datasetPath.toNamespaceKey());
    return datasetService.getVersion(datasetPath, dsConfig.getVirtualDataset().getVersion());
  }

  public static boolean isCreatedFromParent(Transform lastTransform) {
    return lastTransform != null &&
      lastTransform.getType() == TransformType.createFromParent &&
      lastTransform.getTransformCreateFromParent().getCreateFrom().getType() == FromType.Table;
  }


  public static List<Field> getFieldsFromDatasetConfig(DatasetConfig datasetConfig) {
    List<Field> fields = Lists.newArrayList();
    DatasetType datasetType = datasetConfig.getType();

    switch (datasetType) {
      case VIRTUAL_DATASET:
        final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();
        List<ViewFieldType> sqlFieldsList = virtualDataset.getSqlFieldsList();
        if (sqlFieldsList == null) {
          return null;
        }

        for (ViewFieldType fieldType : sqlFieldsList) {
          DataType dataType = DataTypeUtil.getDataType(SqlTypeName.get(fieldType.getType()));
          fields.add(new Field(fieldType.getName(), dataType));
        }
        break;

      case PHYSICAL_DATASET:
      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER:
      case PHYSICAL_DATASET_SOURCE_FILE:
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        final ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
        if (schemaBytes == null) {
          return null;
        }

        final BatchSchema batchSchema = BatchSchema.deserialize(schemaBytes);
        for (int i = 0; i < batchSchema.getFieldCount(); i++) {
          final org.apache.arrow.vector.types.pojo.Field field = batchSchema.getColumn(i);
          if (!NamespaceTable.SYSTEM_COLUMNS.contains(field.getName())) {
            DataType dataType = DataTypeUtil.getDataType(MajorTypeHelper.getMajorTypeForField(field));
            fields.add(new Field(field.getName(), dataType));
          }
        }
        break;

      default:
        throw new UnsupportedOperationException(String.format("The dataset type %s is not supported", datasetType.toString()));
    }

    return fields;
  }

  public static List<org.apache.arrow.vector.types.pojo.Field> getArrowFieldsFromDatasetConfig(DatasetConfig datasetConfig) {
    List<org.apache.arrow.vector.types.pojo.Field> fields = Lists.newArrayList();
    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
    if (schemaBytes == null) {
      return null;
    }

    final BatchSchema batchSchema = BatchSchema.deserialize(schemaBytes);
    for (int i = 0; i < batchSchema.getFieldCount(); i++) {
      final org.apache.arrow.vector.types.pojo.Field field = batchSchema.getColumn(i);
      if (!NamespaceTable.SYSTEM_COLUMNS.contains(field.getName())) {
        fields.add(field);
      }
    }

    return fields;
  }

  public static Set<String> getPartitionedColumns(DatasetConfig datasetConfig) {
    return datasetConfig.getReadDefinition() != null ?
      toSet(datasetConfig.getReadDefinition().getPartitionColumnsList()) :
      Collections.emptySet();
  }

  public static Set<String> getSortedColumns(DatasetConfig datasetConfig) {
    return datasetConfig.getReadDefinition() != null ?
      toSet(datasetConfig.getReadDefinition().getSortColumnsList()) :
      Collections.emptySet();
  }

  private static Set<String> toSet(List<String> list) {
    return list != null ? new HashSet<>(list) : Collections.emptySet();
  }
}
