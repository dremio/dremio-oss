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

import static com.dremio.service.functions.model.ParameterKind.OPTIONAL;
import static com.dremio.service.functions.model.ParameterKind.REGULAR;
import static com.dremio.service.functions.model.ParameterKind.VARARG;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.FunctionSignatureComparator;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.ParameterKind;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.ParameterTypeHierarchy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public final class FunctionSignatureRowMerger {
  private FunctionSignatureRowMerger() {}

  public static ImmutableList<FunctionSignature> merge(ImmutableList<FunctionSignature> signatures) {
    if (signatures.size() < 2) {
      return signatures;
    }

    Set<FunctionSignature> mergedSignatures;
    ImmutableList<FunctionSignature> toBeMergedSignatures = signatures;

    boolean merged;
    do {
      merged = false;
      mergedSignatures = new HashSet<>();
      for (int i = 0; i < toBeMergedSignatures.size(); i++) {
        FunctionSignature left = toBeMergedSignatures.get(i);

        boolean mergedLeft = false;
        for (int j = 0; !mergedLeft && (j < toBeMergedSignatures.size()); j++) {
          if (i == j) {
            continue;
          }

          FunctionSignature right = toBeMergedSignatures.get(j);

          Optional<FunctionSignature> optionalMergedSignature = MergeRule.RULES
            .stream()
            .map(mergeRule -> mergeRule.tryMerge(left, right))
            .filter(optional -> optional.isPresent())
            .map(optional -> optional.get())
            .findFirst();
          if (optionalMergedSignature.isPresent()) {
            mergedSignatures.add(optionalMergedSignature.get());
            mergedLeft = true;
          }
        }

        merged |= mergedLeft;
        if (!mergedLeft) {
          mergedSignatures.add(left);
        }
      }

      toBeMergedSignatures = ImmutableList.copyOf(mergedSignatures);
    } while (merged);

    Set<FunctionSignature> orderedSignatures = new TreeSet<FunctionSignature>(FunctionSignatureComparator.INSTANCE);
    orderedSignatures.addAll(mergedSignatures);

    return ImmutableList.copyOf(orderedSignatures);
  }

  private abstract static class MergeRule {
    private static ImmutableList<MergeRule> RULES = ImmutableList.of(
      KindPrecedenceMergeRule.INSTANCE,
      CommonRegularPrefixMergeRule.INSTANCE,
      VaradicArgPrefixMergeRule.INSTANCE,
      PreferNarrowReturnTypeMergeRule.INSTANCE);

    public Optional<FunctionSignature> tryMerge(FunctionSignature left, FunctionSignature right) {
      if (left.getParameters().isEmpty() || right.getParameters().isEmpty()) {
        return Optional.empty();
      }

      Optional<FunctionSignature> firstAttempt = tryMergeImplementation(left, right);
      if (firstAttempt.isPresent()) {
        return firstAttempt;
      }

      return tryMergeImplementation(right, left);
    }

    protected abstract Optional<FunctionSignature> tryMergeImplementation(FunctionSignature left, FunctionSignature right);
  }

  /*
    MERGE(FOO(A, B), FOO(A, B)) -> FOO(A, B)
    MERGE(FOO(A, B), FOO(A, B?)) -> FOO(A, B?)
    MERGE(FOO(A, B), FOO(A, B...)) -> FOO(A, B...)
    MERGE(FOO(A, B?), FOO(A, B)) -> FOO(A, B?)
    MERGE(FOO(A, B?), FOO(A, B?)) -> FOO(A, B?)
    MERGE(FOO(A, B?), FOO(A, B...)) -> FOO(A, B...)
    MERGE(FOO(A, B...), FOO(A, B)) -> FOO(A, B...)
    MERGE(FOO(A, B...), FOO(A, B?)) -> FOO(A, B...)
    MERGE(FOO(A, B...), FOO(A, B...)) -> FOO(A, B...)
   */
  private static final class KindPrecedenceMergeRule extends MergeRule {
    public static final KindPrecedenceMergeRule INSTANCE = new KindPrecedenceMergeRule();

    private KindPrecedenceMergeRule() {}

    @Override
    protected Optional<FunctionSignature> tryMergeImplementation(FunctionSignature left, FunctionSignature right) {
      if (left.getReturnType() != right.getReturnType()) {
        return Optional.empty();
      }

      boolean sameSize = left.getParameters().size() == right.getParameters().size();
      if (!sameSize) {
        return Optional.empty();
      }

      List<ParameterType> leftTypes = left.getParameters().stream().map(parameter -> parameter.getType()).collect(Collectors.toList());
      List<ParameterType> rightTypes = right.getParameters().stream().map(parameter -> parameter.getType()).collect(Collectors.toList());
      if (!leftTypes.equals(rightTypes)) {
        return Optional.empty();
      }

      ParameterKind leftKind = Iterables.getLast(left.getParameters()).getKind();
      ParameterKind rightKind = Iterables.getLast(right.getParameters()).getKind();
      FunctionSignature winningSignature;
      switch (leftKind) {
      case REGULAR:
        winningSignature = right;
        break;

      case OPTIONAL:
        winningSignature = rightKind == VARARG ? right : left;
        break;

      case VARARG:
        winningSignature = left;
        break;

      default:
        throw new UnsupportedOperationException();
      }

      return Optional.of(winningSignature);
    }
  }

  /*
      MERGE(FOO(A), FOO(A, B)) -> FOO(A, B?)
      MERGE(FOO(A), FOO(A, B?)) -> FOO(A, B?)
      MERGE(FOO(A), FOO(A, B...)) -> FOO(A, B...)
      MERGE(FOO(A, B), FOO(A, B, B)) -> FOO(A, B), FOO(A, B, B?)
      MERGE(FOO(A, B), FOO(A, B, B?)) -> FOO(A, B, B?)
      MERGE(FOO(A, B), FOO(A, B, B..)) -> FOO(A, B, B...)
   */
  private static final class CommonRegularPrefixMergeRule extends MergeRule {
    public static final CommonRegularPrefixMergeRule INSTANCE = new CommonRegularPrefixMergeRule();

    private CommonRegularPrefixMergeRule() {}

    @Override
    protected Optional<FunctionSignature> tryMergeImplementation(FunctionSignature left, FunctionSignature right) {
      if (left.getReturnType() != right.getReturnType()) {
        return Optional.empty();
      }

      if (right.getParameters().size() != left.getParameters().size() + 1) {
        return Optional.empty();
      }

      List<Parameter> leftParameters = left.getParameters();
      List<Parameter> rightParametersPrefix = right.getParameters().subList(0, right.getParameters().size() - 1);

      if (!leftParameters.stream().allMatch(parameter -> parameter.getKind() == REGULAR)) {
        return Optional.empty();
      }

      if (!rightParametersPrefix.stream().allMatch(parameter -> parameter.getKind() == REGULAR)) {
        return Optional.empty();
      }

      List<ParameterType> leftTypes = leftParameters
        .stream()
        .map(parameter -> parameter.getType())
        .collect(Collectors.toList());
      List<ParameterType> rightPrefixTypes = rightParametersPrefix
        .stream()
        .map(parameter -> parameter.getType())
        .collect(Collectors.toList());
      if (!leftTypes.equals(rightPrefixTypes)) {
        return Optional.empty();
      }

      Parameter rightLastParameter = Iterables.getLast(right.getParameters());
      ParameterKind rightLastKind = rightLastParameter.getKind();
      if (rightLastKind != REGULAR) {
        return Optional.of(right);
      }

      List<Parameter> mergedParameters = new ArrayList<>(rightParametersPrefix);
      mergedParameters.add(Parameter.builder()
        .kind(OPTIONAL)
        .type(rightLastParameter.getType())
        .name(rightLastParameter.getName())
        .build());
      FunctionSignature functionSignature = FunctionSignature.builder()
        .returnType(right.getReturnType())
        .addAllParameters(mergedParameters)
        .build();

      return Optional.of(functionSignature);
    }
  }

  /*
      MERGE(FOO(A, B...), FOO(A, B, B)) -> FOO(A, B...)
   */
  private static final class VaradicArgPrefixMergeRule extends MergeRule {
    public static final VaradicArgPrefixMergeRule INSTANCE = new VaradicArgPrefixMergeRule();

    private VaradicArgPrefixMergeRule() {}

    @Override
    protected Optional<FunctionSignature> tryMergeImplementation(FunctionSignature left, FunctionSignature right) {
      if (left.getReturnType() != right.getReturnType()) {
        return Optional.empty();
      }

      if (!(right.getParameters().size() > left.getParameters().size())) {
        return Optional.empty();
      }

      List<Parameter> leftParameters = left.getParameters();
      List<Parameter> rightParameters = right.getParameters();

      if (leftParameters.size() < 1) {
        return Optional.empty();
      }

      Parameter lastLeftParameter = Iterables.getLast(leftParameters);
      if (lastLeftParameter.getKind() != VARARG) {
        return Optional.empty();
      }

      List<ParameterType> leftPrefixTypes = leftParameters
        .subList(0, leftParameters.size() - 1)
        .stream()
        .map(parameter -> parameter.getType())
        .collect(Collectors.toList());
      List<ParameterType> rightPrefixTypes = rightParameters
        .subList(0, leftParameters.size() - 1)
        .stream()
        .map(parameter -> parameter.getType())
        .collect(Collectors.toList());
      if (!leftPrefixTypes.equals(rightPrefixTypes)) {
        return Optional.empty();
      }

      List<Parameter> rightSuffix = rightParameters.subList(leftParameters.size(), rightParameters.size());
      if (!rightSuffix.stream().allMatch(parameter -> parameter.getType() == lastLeftParameter.getType())) {
        return Optional.empty();
      }

      return Optional.of(left);
    }
  }

  /*
      MERGE(FOO(ANY) -> BLAH, FOO(INT) -> ANY) -> FOO(ANY) -> BLAH
   */
  private static final class PreferNarrowReturnTypeMergeRule extends MergeRule {
    public static final PreferNarrowReturnTypeMergeRule INSTANCE = new PreferNarrowReturnTypeMergeRule();

    private PreferNarrowReturnTypeMergeRule() {}

    @Override
    protected Optional<FunctionSignature> tryMergeImplementation(FunctionSignature left, FunctionSignature right) {
      if (left.getReturnType() == right.getReturnType()) {
        return Optional.empty();
      }

      if (left.getParameters().size() != right.getParameters().size()) {
        return Optional.empty();
      }

      // The return type of left needs to be more specific than the right
      if (!ParameterTypeHierarchy.isDescendantOf(right.getReturnType(), left.getReturnType())) {
        return Optional.empty();
      }

      // All the parameters of the right need to be more specific than the left.
      for (int i = 0; i < left.getParameters().size(); i++) {
        Parameter leftParameter = left.getParameters().get(i);
        Parameter rightParameter = right.getParameters().get(i);

        if (leftParameter.getKind() != rightParameter.getKind()) {
          return Optional.empty();
        }

        if (!ParameterTypeHierarchy.isDescendantOf(leftParameter.getType(), rightParameter.getType())) {
          return Optional.empty();
        }
      }

      return Optional.of(left);
    }
  }
}
