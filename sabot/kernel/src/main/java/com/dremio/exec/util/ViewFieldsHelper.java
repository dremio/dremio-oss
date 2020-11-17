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
package com.dremio.exec.util;

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import java.util.List;

import org.apache.arrow.flatbuf.Field;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.collect.Lists;
import com.google.flatbuffers.FlatBufferBuilder;

import io.protostuff.ByteString;

/**
 * Extract view fields from dataset
 */
public final class ViewFieldsHelper {

  /**
   * Get view fields from dataset config as it is in the batch schema (if not present, defaults to reltree's row type)
   * @param config dataset config
   * @return List of view fields
   */
  public static List<ViewFieldType> getViewFields(DatasetConfig config) {
    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(config);
    final boolean virtualDataset = config.getType() == DatasetType.VIRTUAL_DATASET;
    if (schemaBytes == null && virtualDataset) {
      return config.getVirtualDataset().getSqlFieldsList();
    }
    if (schemaBytes != null) {
      return getBatchSchemaFields(BatchSchema.deserialize(schemaBytes.toByteArray()));
    }
    return null;
  }

  /**
   * Get view fields from dataset config as it is in planner's reltree's row type.
   * @param config dataset config
   * @return List of view fields
   */
  public static List<ViewFieldType> getCalciteViewFields(DatasetConfig config) {
    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final List<ViewFieldType> calciteFields = config.getVirtualDataset().getCalciteFieldsList();
      if (calciteFields != null && !calciteFields.isEmpty()) {
        return calciteFields;
      }

      final List<ViewFieldType> sqlFields = config.getVirtualDataset().getSqlFieldsList();
      if (sqlFields != null && !sqlFields.isEmpty()) {
        return sqlFields;
      }
    }
    return null;
  }

  public static List<ViewFieldType> getBatchSchemaFields(final BatchSchema batchSchema) {
    final List<ViewFieldType> fields = Lists.newArrayList();
    for (int i = 0; i < batchSchema.getFieldCount(); i++) {
      final org.apache.arrow.vector.types.pojo.Field field = batchSchema.getColumn(i);
      final ViewFieldType viewField =
        new ViewFieldType(field.getName(), TypeInferenceUtils.getCalciteTypeFromMinorType(MajorTypeHelper.getMajorTypeForField(field).getMinorType()).toString());

      final CompleteType completeType = CompleteType.fromField(field);
      final SqlTypeName sqlTypeName = TypeInferenceUtils.getCalciteTypeFromMinorType(getMajorTypeForField(field).getMinorType());

      viewField.setPrecision(completeType.getPrecision());
      viewField.setScale(completeType.getScale());
      viewField.setIsNullable(true);
      viewField.setTypeFamily(sqlTypeName.getFamily().toString());
      viewField.setSerializedField(ByteString.copyFrom(serializeField(field)));

      // TODO (AH)
      //viewField.setStartUnit();
      //viewField.setEndUnit();
      //viewField.setFractionalSecondPrecision();
      fields.add(viewField);
    }
    return fields;
  }

  public static byte[] serializeField(org.apache.arrow.vector.types.pojo.Field field) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(field.getField(builder));
    return builder.sizedByteArray();
  }

  public static org.apache.arrow.vector.types.pojo.Field deserializeField(ByteString bytes) {
    Field field = Field.getRootAsField(bytes.asReadOnlyByteBuffer());
    org.apache.arrow.vector.types.pojo.Field f = org.apache.arrow.vector.types.pojo.Field.convertField(field);
    return f;
  }
}
