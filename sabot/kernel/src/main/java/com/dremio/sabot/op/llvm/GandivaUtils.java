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
package com.dremio.sabot.op.llvm;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.record.VectorAccessible;
import com.google.common.collect.ImmutableList;

public class GandivaUtils {

  /**
   * Creates the vector schema from incoming container and referenced fields.
   * @param input
   * @param referencedFields
   * @return the vector schema root.
   */
  public static VectorSchemaRoot getSchemaRoot(VectorAccessible input, Set referencedFields) {
    List<FieldVector> fv = ImmutableList.copyOf(input)
      .stream()
      .map(vw -> ((FieldVector)vw.getValueVector()))
      .filter(fVec -> referencedFields.contains(fVec.getField()))
      .collect(Collectors.toList());

    List<Field> fields = fv.stream()
      .map(fieldVec -> fieldVec.getField())
      .collect(Collectors.toList());

    Schema schemaWithOnlyReferencedFields = new Schema(fields);
    VectorSchemaRoot root = new VectorSchemaRoot(
      schemaWithOnlyReferencedFields,
      fv,
      0
    );
    return root;
  }
}
