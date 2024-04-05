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
package com.dremio.common.types;

import com.dremio.common.expression.CompleteType;
import java.util.Optional;

/**
 * Contains the additional list of the supported coercions between a Mongo source {@link
 * CompleteType} and the destination table {@link CompleteType}.
 */
public class TypeCoercionRulesComplexAndIncompatibleToVarchar extends TypeCoercionRules {
  @Override
  public Optional<CompleteType> getResultantType(CompleteType fileType, CompleteType tableType) {
    Optional<CompleteType> result = super.getResultantType(fileType, tableType);
    if (result.isPresent()) {
      return result;
    } else if (!fileType.getType().equals(tableType.getType())) {
      // At this point in code we have exhausted all up promotion and coercion rules and result is
      // still empty
      return Optional.of(CompleteType.VARCHAR);
    }
    return Optional.empty();
  }
}
