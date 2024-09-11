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
package com.dremio.exec.store.iceberg.dremioudf.api.udf;

import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public interface UdfSignature {
  /**
   * Get the ID of the UDF signature.
   *
   * @return he ID of the UDF signature.
   */
  String signatureId();

  /**
   * Get List of parameters.
   *
   * @return List of parameters.
   */
  List<Types.NestedField> parameters();

  /**
   * Get the return data type.
   *
   * @return the eturn data type.
   */
  Type returnType();

  /**
   * Get if the UDF is deterministic. If true, The UDF returns the same result giving the same
   * parameters and has no side effects
   *
   * @return true if the UDF is deterministic.
   */
  boolean deterministic();

  default boolean sameSignature(UdfSignature anotherSignature) {
    return parameters().equals(anotherSignature.parameters())
        && returnType().equals(anotherSignature.returnType())
        && deterministic() == anotherSignature.deterministic();
  }
}
