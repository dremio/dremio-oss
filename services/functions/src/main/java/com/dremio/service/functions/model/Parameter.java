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
package com.dremio.service.functions.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;
import org.immutables.value.Value;

/** Information about a function parameter in a function signature. */
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

  public abstract ParameterKind getKind();

  public abstract ParameterType getType();

  @Nullable
  public abstract String getName();

  @Nullable
  public abstract String getDescription();

  @Nullable
  public abstract String getFormat();

  public static ImmutableParameter.KindBuildStage builder() {
    return ImmutableParameter.builder();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder().append(getType());

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

    String description = getDescription();
    if (description != null) {
      stringBuilder.append("[").append(description).append("]");
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

    ImmutableParameter.BuildFinal builder = Parameter.builder().kind(kind).type(type).name("");

    if (description != null) {
      builder = builder.description(description);
    }

    return builder.build();
  }
}
