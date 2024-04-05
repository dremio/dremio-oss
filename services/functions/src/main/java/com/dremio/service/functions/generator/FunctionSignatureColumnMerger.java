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
package com.dremio.service.functions.generator;

import static java.util.stream.Collectors.groupingBy;

import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.ParameterKind;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.ParameterTypeHierarchy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class FunctionSignatureColumnMerger {
  private FunctionSignatureColumnMerger() {}

  public static ImmutableList<FunctionSignature> merge(
      ImmutableList<FunctionSignature> signatures) {
    Map<Integer, List<FunctionSignature>> signaturesByLength =
        signatures.stream()
            .collect(groupingBy(functionSignature -> functionSignature.getParameters().size()));

    List<FunctionSignature> mergedSignatures = new ArrayList<>();
    for (Integer length : signaturesByLength.keySet()) {
      ImmutableList<FunctionSignature> signaturesOfLength =
          ImmutableList.copyOf(signaturesByLength.get(length));
      for (int columnToMerge = 0; columnToMerge < length; columnToMerge++) {
        signaturesOfLength = merge(signaturesOfLength, columnToMerge);
      }

      mergedSignatures.addAll(signaturesOfLength);
    }

    return ImmutableList.copyOf(mergedSignatures);
  }

  private static ImmutableList<FunctionSignature> merge(
      ImmutableList<FunctionSignature> signatures, int columnToMerge) {
    Map<MetaData, List<FunctionSignature>> groupedSignatures =
        signatures.stream()
            .collect(
                groupingBy(functionSignature -> MetaData.create(functionSignature, columnToMerge)));

    List<FunctionSignature> newSignatures = new ArrayList<>();
    for (MetaData metaData : groupedSignatures.keySet()) {
      if (metaData.isEmpty) {
        newSignatures.addAll(groupedSignatures.get(metaData));
        continue;
      }

      ImmutableSet<ParameterType> columnParameterTypes =
          groupedSignatures.get(metaData).stream()
              .map(
                  functionSignature ->
                      functionSignature.getParameters().get(columnToMerge).getType())
              .collect(ImmutableSet.toImmutableSet());

      ImmutableSet<ParameterType> typeGroupings = getTypeGroupings(columnParameterTypes);
      for (ParameterType parameterType : typeGroupings) {
        List<Parameter> parameters = new ArrayList<>();
        parameters.addAll(metaData.prefix);
        parameters.add(
            Parameter.builder().kind(metaData.columnKind).type(parameterType).name("").build());
        parameters.addAll(metaData.suffix);
        FunctionSignature functionSignature =
            FunctionSignature.builder()
                .returnType(metaData.returnType)
                .addAllParameters(parameters)
                .build();
        newSignatures.add(functionSignature);
      }
    }

    return ImmutableList.copyOf(newSignatures);
  }

  private static ImmutableSet<ParameterType> getTypeGroupings(
      ImmutableSet<ParameterType> parameterTypes) {
    if (parameterTypes.contains(ParameterType.ANY)) {
      return ImmutableSet.of(ParameterType.ANY);
    }

    ImmutableSet.Builder<ParameterType> builder = new ImmutableSet.Builder<>();

    for (ParameterType parameterType : parameterTypes) {
      ParameterType parentType = ParameterTypeHierarchy.getParent(parameterType).get();
      ImmutableSet<ParameterType> siblings = ParameterTypeHierarchy.getChildren(parentType).get();

      boolean hasAllSiblings = parameterTypes.containsAll(siblings);
      if (hasAllSiblings) {
        builder.add(parentType);
        ImmutableSet<ParameterType> withoutSiblings =
            Sets.difference(parameterTypes, siblings).immutableCopy();
        builder.addAll(getTypeGroupings(withoutSiblings));
      } else {
        builder.add(parameterType);
      }
    }

    ImmutableSet<ParameterType> groups = builder.build();
    if (!groups.equals(parameterTypes)) {
      return getTypeGroupings(groups);
    }

    return groups;
  }

  private static final class MetaData {
    private final boolean isEmpty;
    private final List<Parameter> prefix;
    private final List<Parameter> suffix;
    private final ParameterKind columnKind;
    private final ParameterType returnType;

    public MetaData(
        boolean isEmpty,
        List<Parameter> prefix,
        List<Parameter> suffix,
        ParameterKind columnKind,
        ParameterType returnType) {
      this.isEmpty = isEmpty;
      this.prefix = prefix;
      this.suffix = suffix;
      this.columnKind = columnKind;
      this.returnType = returnType;
    }

    public static MetaData create(FunctionSignature functionSignature, int columnToMerge) {
      if (functionSignature.getParameters().isEmpty()) {
        return new MetaData(
            true,
            ImmutableList.of(),
            ImmutableList.of(),
            ParameterKind.REGULAR,
            functionSignature.getReturnType());
      }

      List<Parameter> prefix = functionSignature.getParameters().subList(0, columnToMerge);
      List<Parameter> suffix =
          functionSignature
              .getParameters()
              .subList(columnToMerge + 1, functionSignature.getParameters().size());
      ParameterType returnType = functionSignature.getReturnType();
      ParameterKind columnKind = functionSignature.getParameters().get(columnToMerge).getKind();

      return new MetaData(false, prefix, suffix, columnKind, returnType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MetaData metaData = (MetaData) o;
      return isEmpty == metaData.isEmpty
          && prefix.equals(metaData.prefix)
          && suffix.equals(metaData.suffix)
          && columnKind == metaData.columnKind
          && returnType == metaData.returnType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isEmpty, prefix, suffix, columnKind, returnType);
    }
  }
}
