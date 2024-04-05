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
package com.dremio.exec.vector;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.util.VectorUtil;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.arrow.vector.VarBinaryVector;

/**
 * Holds the vector only if it's part of the schema. Get/Set is optionally done based on the same
 * condition.
 */
public class OptionalVarBinaryVectorHolder {
  final Optional<VarBinaryVector> vector;

  public OptionalVarBinaryVectorHolder(VectorAccessible vectorAccessible, String schemaPath) {
    TypedFieldId typedFieldId =
        vectorAccessible.getSchema().getFieldId(SchemaPath.getSimplePath(schemaPath));
    vector =
        typedFieldId == null
            ? Optional.empty()
            : Optional.of(
                (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(vectorAccessible, schemaPath));
  }

  /**
   * @param row
   * @return Optional.empty if field is not part of schema, optional with value otherwise.
   */
  public Optional<byte[]> get(int row) {
    return vector.map(v -> v.get(row));
  }

  /**
   * Sets the value from the supplier if the field is part of the schema. NOOP otherwise.
   *
   * @param row
   * @param valueSupplier
   */
  public void setSafe(int row, Supplier<byte[]> valueSupplier) {
    vector.ifPresent(v -> v.setSafe(row, valueSupplier.get()));
  }
}
