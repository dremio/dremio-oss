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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Datastructure for Function type inference algorithms.
 */
public final class Function {
  private final String name;
  private final ImmutableList<FunctionSignature> signatures;
  private final String description;
  private final String syntax;

  @JsonCreator
  public Function(
    @JsonProperty("name") String name,
    @JsonProperty("signatures") ImmutableList<FunctionSignature> signatures,
    @JsonProperty("description") String description,
    @JsonProperty("syntax") String syntax) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(signatures);

    this.name = name;
    this.signatures = signatures;
    this.description = description;
    this.syntax = syntax;
  }

  public String getName() {
    return name;
  }

  public ImmutableList<FunctionSignature> getSignatures() {
    return signatures;
  }

  public String getDescription() { return description; }

  public String getSyntax() {
    return syntax;
  }

  public static Function mergeOverloads(List<Function> overloads) {
    Preconditions.checkNotNull(overloads);
    Preconditions.checkArgument(!overloads.isEmpty());

    Set<FunctionSignature> signatures = new TreeSet<>(FunctionSignatureComparator.INSTANCE);
    for (Function overload : overloads) {
      signatures.addAll(overload.getSignatures());
    }

    Function firstOverload = overloads.get(0);
    return new Function(
      firstOverload.getName(),
      ImmutableList.copyOf(signatures),
      firstOverload.getDescription(),
      firstOverload.getSyntax());
  }
}
