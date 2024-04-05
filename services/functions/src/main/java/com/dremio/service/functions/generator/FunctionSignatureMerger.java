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

import com.dremio.service.functions.model.FunctionSignature;
import com.google.common.collect.ImmutableList;

public final class FunctionSignatureMerger {
  private FunctionSignatureMerger() {}

  public static ImmutableList<FunctionSignature> merge(
      ImmutableList<FunctionSignature> signatures) {
    if (signatures.size() < 2) {
      return signatures;
    }

    signatures = FunctionSignatureColumnMerger.merge(signatures);
    signatures = FunctionSignatureRowMerger.merge(signatures);

    return signatures;
  }
}
