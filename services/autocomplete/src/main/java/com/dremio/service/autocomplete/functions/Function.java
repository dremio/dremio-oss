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

  @JsonCreator
  public Function(
    @JsonProperty("name") String name,
    @JsonProperty("signatures") ImmutableList<FunctionSignature> signatures) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(signatures);

    this.name = name;
    this.signatures = signatures;
  }

  public String getName() {
    return name;
  }

  public ImmutableList<FunctionSignature> getSignatures() {
    return signatures;
  }
}
