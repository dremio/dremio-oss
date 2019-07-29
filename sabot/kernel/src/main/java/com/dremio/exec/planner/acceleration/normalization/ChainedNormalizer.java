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
package com.dremio.exec.planner.acceleration.normalization;

import org.apache.calcite.rel.RelNode;

import com.google.common.base.Preconditions;

/**
 * chained normalizer
 */
public class ChainedNormalizer implements Normalizer {

  private final Normalizer[] normalizers;

  public ChainedNormalizer(Normalizer... normalizers) {
    this.normalizers = Preconditions.checkNotNull(normalizers);
  }

  public static ChainedNormalizer of(Normalizer... normalizers) {
    return new ChainedNormalizer(normalizers);
  }

  @Override
  public RelNode normalize(RelNode query) {
    RelNode temp = query;
    for (Normalizer normalizer : normalizers) {
      temp = normalizer.normalize(temp);
    }
    return temp;
  }
}
