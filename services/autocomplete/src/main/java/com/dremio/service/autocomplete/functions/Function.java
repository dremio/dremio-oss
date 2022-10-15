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

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;

/**
 * Datastructure for Function type inference algorithms.
 */
@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableFunction.class)
@JsonDeserialize(as = ImmutableFunction.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class Function {
  public abstract String getName();
  public abstract ImmutableList<FunctionSignature> getSignatures();
  public abstract Optional<String> getDescription();

  public static ImmutableFunction.NameBuildStage builder() {
    return ImmutableFunction.builder();
  }
}
