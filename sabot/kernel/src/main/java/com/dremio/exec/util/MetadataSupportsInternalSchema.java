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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.record.BatchSchema;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;

/** Implements schema merging APIs which help Metadata retrieval to determine schema */
public interface MetadataSupportsInternalSchema extends SupportsTypeCoercionsAndUpPromotions {

  default BatchSchema getMergedSchema(
      BatchSchema oldSchema,
      BatchSchema newSchema,
      boolean isSchemaLearningEnabled,
      List<Field> droppedFields,
      List<Field> modifiedFields,
      boolean isInternalSchemaSupportKeyEnabled,
      List<String> datasetPath,
      String filePath) {
    try {
      newSchema = newSchema != null ? newSchema.removeNullFields().handleUnions(this) : null;
      oldSchema = oldSchema != null ? oldSchema.removeNullFields().handleUnions(this) : null;

      if (isInternalSchemaSupportKeyEnabled) {

        if (oldSchema == null) {
          return newSchema;
        }

        if (!isSchemaLearningEnabled) {
          return oldSchema;
        }

        if (droppedFields != null && modifiedFields != null) {
          BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
          BatchSchemaDiff diff =
              batchSchemaDiffer.diff(oldSchema.getFields(), newSchema.getFields());

          boolean schemaNotChanged =
              diff.getModifiedFields().equals(modifiedFields)
                  && diff.getDroppedFields().equals(droppedFields)
                  && diff.getAddedFields().isEmpty();

          if (schemaNotChanged) {
            return oldSchema;
          }

          // we have to apply modifications to the schema
          BatchSchema addedSchema = new BatchSchema(diff.getAddedFields());
          addedSchema = addedSchema.difference(new BatchSchema(droppedFields));
          addedSchema = addedSchema.difference(new BatchSchema(modifiedFields));

          // we have to apply modifications to the schema
          BatchSchema modifiedSchema = new BatchSchema(diff.getModifiedFields());
          modifiedSchema = modifiedSchema.difference(new BatchSchema(droppedFields));
          modifiedSchema = modifiedSchema.difference(new BatchSchema(modifiedFields));

          // remove any columns where there is a struct. We don't do schema learning on top level
          // structs
          for (Field field : modifiedFields) {
            // only for non complex columns change the type
            if (!field.getChildren().isEmpty()) {
              addedSchema = addedSchema.dropField(field.getName());
              modifiedSchema = modifiedSchema.dropField(field.getName());
            }
          }
          // add the added fields
          oldSchema = oldSchema.mergeWithUpPromotion(addedSchema, this);
          oldSchema = oldSchema.mergeWithUpPromotion(modifiedSchema, this);
        }
        return oldSchema;
      } else {
        newSchema = oldSchema != null ? oldSchema.mergeWithUpPromotion(newSchema, this) : newSchema;
        return newSchema;
      }
    } catch (NoSupportedUpPromotionOrCoercionException e) {
      e.addFilePath(filePath);
      e.addDatasetPath(datasetPath);
      throw UserException.unsupportedError(e).message(e.getMessage()).build();
    }
  }
}
