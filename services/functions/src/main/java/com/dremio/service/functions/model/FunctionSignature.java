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

import com.dremio.service.functions.snippets.Snippet;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** The signature for a function. */
@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableFunctionSignature.class)
@JsonDeserialize(as = ImmutableFunctionSignature.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class FunctionSignature {
  public abstract ParameterType getReturnType();

  public abstract ImmutableList<Parameter> getParameters();

  @Nullable
  public abstract String getDescription();

  @Nullable
  public abstract List<SampleCode> getSampleCodes();

  @Nullable
  public abstract Snippet getSnippetOverride();

  public static ImmutableFunctionSignature.ReturnTypeBuildStage builder() {
    return ImmutableFunctionSignature.builder();
  }

  public static FunctionSignature create(ParameterType returnType, Parameter... parameters) {
    return builder().returnType(returnType).addParameters(parameters).build();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder().append("(");

    for (int i = 0; i < getParameters().size(); i++) {
      if (i != 0) {
        stringBuilder.append(", ");
      }

      Parameter parameter = getParameters().get(i);
      stringBuilder.append(parameter);
    }

    stringBuilder.append(")").append(" -> ").append(getReturnType());

    Snippet snippetOverride = getSnippetOverride();
    if (getSnippetOverride() != null) {
      stringBuilder.append("\n\t").append(snippetOverride);
    }

    return stringBuilder.toString();
  }

  public static FunctionSignature parse(String text) {
    String[] preSnippetAndPostSnippet = text.split("\n\t");
    Snippet snippetOverride;
    if (preSnippetAndPostSnippet.length != 1) {
      snippetOverride = Snippet.tryParse(preSnippetAndPostSnippet[1]).orElse(null);
    } else {
      snippetOverride = null;
    }

    String paramsAndReturnTypeText = preSnippetAndPostSnippet[0];
    String[] paramsAndReturnType = paramsAndReturnTypeText.split(" -> ");
    String params = paramsAndReturnType[0];
    String returnTypeString = paramsAndReturnType[1];
    params = params.substring(1, params.length() - 1);
    List<Parameter> parameterList = new ArrayList<Parameter>();
    if (!params.isEmpty()) {
      String[] parametersText = params.split(", ");
      for (String parameterText : parametersText) {
        Parameter parameter = Parameter.parse(parameterText);
        parameterList.add(parameter);
      }
    }

    return FunctionSignature.builder()
        .returnType(ParameterType.valueOf(returnTypeString))
        .addAllParameters(parameterList)
        .snippetOverride(snippetOverride)
        .build();
  }
}
