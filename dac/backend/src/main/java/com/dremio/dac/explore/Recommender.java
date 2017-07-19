/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.explore;

import java.util.List;

import com.dremio.dac.proto.model.dataset.DataType;

/**
 * Generic transform recommender interface.
 *
 * @param <T>
 */
public abstract class Recommender<T, U> {

  /**
   * Get the list of recommender rules for given selection.
   *
   * @param selection User selection in UI
   * @param selColType selected column type
   *
   * @return
   */
  public abstract List<T> getRules(U selection, DataType selColType);

  /**
   * Get a {@link TransformRuleWrapper<T>} around the given rule.
   * @return
   */
  public abstract TransformRuleWrapper<T> wrapRule(T rule);


  /**
   * Wrapper around transform recommendation rule which provides methods to provide evaluating the rule.
   * @param <T>
   */
  public abstract static class TransformRuleWrapper<T> {

    public abstract String getMatchFunctionExpr(String input);

    public boolean canGenerateExamples() {
      return true;
    }

    public abstract String getExampleFunctionExpr(String input);

    public String getFunctionExpr(String expr, Object... args) {
      return null;
    }

    public abstract T getRule();

    public abstract String describe();
  }
}
