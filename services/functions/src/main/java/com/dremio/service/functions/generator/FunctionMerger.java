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

import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionSignature;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class FunctionMerger {
  private FunctionMerger() {}

  public static Function merge(ImmutableList<Function> functions) {
    Preconditions.checkNotNull(functions);
    Preconditions.checkArgument(!functions.isEmpty());

    ImmutableSet.Builder<FunctionSignature> signaturesBuilder = new ImmutableSet.Builder<>();
    for (Function function : functions) {
      if (function.getSignatures() != null) {
        signaturesBuilder.addAll(function.getSignatures());
      }
    }

    ImmutableList<FunctionSignature> mergedSignatures =
        FunctionSignatureMerger.merge(signaturesBuilder.build().asList());

    Function firstFunction = functions.get(0);
    return Function.builder()
        .name(firstFunction.getName())
        .addAllSignatures(mergedSignatures)
        .description(firstFunction.getDescription())
        .build();
  }
}
