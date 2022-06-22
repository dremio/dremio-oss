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

import com.google.common.collect.ImmutableList;

/**
 * The context for what function we are in and how far we are in it.
 */
public final class FunctionContext {
  private final Function function;
  private final ImmutableList<SqlTypeName> typesUsedSoFar;
  private final ImmutableList<FunctionSignature> signaturesMatched;

  public FunctionContext(
    Function function,
    ImmutableList<SqlTypeName> typesUsedSoFar,
    ImmutableList<FunctionSignature> signaturesMatched) {
    this.function = function;
    this.typesUsedSoFar = typesUsedSoFar;
    this.signaturesMatched = signaturesMatched;
  }

  public Function getFunction() {
    return function;
  }

  public ImmutableList<SqlTypeName> getTypesUsedSoFar() {
    return typesUsedSoFar;
  }
  public ImmutableList<FunctionSignature> getSignaturesMatched() {
    return signaturesMatched;
  }
}
