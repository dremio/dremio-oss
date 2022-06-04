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
package com.dremio.service.autocomplete.functions;

import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Signature for a function that holds operand types and corresponding return type.
 */
public final class FunctionSignature {
  private final SqlTypeName returnType;
  private final ImmutableList<SqlTypeName> operandTypes;

  @JsonCreator
  public FunctionSignature(
    @JsonProperty("returnType") SqlTypeName returnType,
    @JsonProperty("operandTypes") ImmutableList<SqlTypeName> operandTypes) {
    Preconditions.checkNotNull(returnType);
    Preconditions.checkNotNull(operandTypes);

    this.returnType = returnType;
    this.operandTypes = operandTypes;
  }

  public SqlTypeName getReturnType() {
    return returnType;
  }

  public ImmutableList<SqlTypeName> getOperandTypes() {
    return operandTypes;
  }
}
