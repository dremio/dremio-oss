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
package com.dremio.service.autocomplete.functionlist2;

import org.immutables.value.Value;

import com.dremio.service.autocomplete.functions.ParameterKind;
import com.dremio.service.autocomplete.functions.ParameterType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
/**
 * Information about a function parameter in a function signature.
 */
@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableParameterInfo.class)
@JsonDeserialize(as = ImmutableParameterInfo.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class ParameterInfo {
  public abstract ParameterKind getKind();
  public abstract ParameterType getType();
  public abstract String getName();
  public abstract Optional<String> getDescription();

  public abstract Optional<String> getFormat();

  public static ImmutableParameterInfo.KindBuildStage builder() {
    return ImmutableParameterInfo.builder();
  }

  public static ParameterInfo create(String name, ParameterKind kind, ParameterType type) {
    return builder()
      .kind(kind)
      .type(type)
      .name(name)
      .build();
  }

  public static ParameterInfo create(String name, ParameterKind kind, ParameterType type, String description, String format) {
    return builder()
      .kind(kind)
      .type(type)
      .name(name)
      .description(description)
      .format(format)
      .build();
  }
}
