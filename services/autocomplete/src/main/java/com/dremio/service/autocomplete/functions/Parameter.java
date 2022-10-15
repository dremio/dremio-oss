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

import javax.ws.rs.NotSupportedException;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableParameter.class)
@JsonDeserialize(as = ImmutableParameter.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class Parameter {
  private static final class SYMBOLS {
    private static String VARARG = "...";
    private static String OPTIONAL = "?";
    private static String REGULAR = "";
  }

  public abstract ParameterType getType();
  public abstract ParameterKind getKind();
  public abstract Optional<String> getDescription();

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder()
      .append(getType());

    String symbol;
    switch (getKind()) {
    case VARARG:
      symbol = SYMBOLS.VARARG;
      break;

    case OPTIONAL:
      symbol = SYMBOLS.OPTIONAL;
      break;

    case REGULAR:
      symbol = SYMBOLS.REGULAR;
      break;

    default:
      throw new NotSupportedException();
    }

    stringBuilder.append(symbol);

    if (getDescription().isPresent()) {
      stringBuilder
        .append("[")
        .append(getDescription().get())
        .append("]");
    }

    return stringBuilder.toString();
  }

  public static Parameter parse(String text) {
    String description = null;
    int startDescription = text.indexOf('[');
    if (startDescription > 0) {
      int endDescription = text.indexOf(']');
      description = text.substring(startDescription + 1, endDescription);
      text = text.substring(0, startDescription);
    }

    ParameterKind kind;
    if (text.endsWith(SYMBOLS.VARARG)) {
      kind = ParameterKind.VARARG;
    } else if (text.endsWith(SYMBOLS.OPTIONAL)) {
      kind = ParameterKind.OPTIONAL;
    } else {
      kind = ParameterKind.REGULAR;
    }

    String parameterTypeText;
    switch (kind) {
    case VARARG:
      parameterTypeText = text.substring(0, text.length() - SYMBOLS.VARARG.length());
      break;

    case OPTIONAL:
      parameterTypeText = text.substring(0, text.length() - SYMBOLS.OPTIONAL.length());
      break;

    case REGULAR:
      parameterTypeText = text.substring(0, text.length() - SYMBOLS.REGULAR.length());
      break;

    default:
      throw new NotSupportedException();
    }

    ParameterType type = ParameterType.valueOf(parameterTypeText);

    ImmutableParameter.BuildFinal builder = Parameter.builder()
      .type(type)
      .kind(kind);
    if (description != null) {
      builder = builder.description(description);
    }

    return builder.build();
  }

  public static Parameter create(ParameterType type, ParameterKind kind) {
    return builder()
      .type(type)
      .kind(kind)
      .build();
  }

  public static Parameter create(ParameterType type, ParameterKind kind, String description) {
    return builder()
      .type(type)
      .kind(kind)
      .description(description)
      .build();
  }

  public static Parameter createRegular(ParameterType type) {
    return create(type, ParameterKind.REGULAR);
  }

  public static Parameter createVarArg(ParameterType type) {
    return create(type, ParameterKind.VARARG);
  }

  public static ImmutableParameter.TypeBuildStage builder() {
    return ImmutableParameter.builder();
  }
}
