/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

package com.dremio.exec.resolver;

import java.util.List;

import com.dremio.common.expression.FunctionCall;
import com.dremio.exec.expr.fn.BaseFunctionHolder;

/**
 * An implementing class of FunctionResolver provide their own algorithm to choose a BaseFunctionHolder from a given list of
 * candidates, with respect to a given FunctionCall
 */
public interface FunctionResolver {
  /**
   * Creates a placeholder SqlFunction for an invocation of a function with a
   * possibly qualified name. This name must be resolved into either a builtin
   * function or a user-defined function.
   *
   * @param methods   a list of candidates of BaseFunctionHolder to be chosen from
   * @param call      a given function call whose BaseFunctionHolder is to be determined via this method
   * @return BaseFunctionHolder the chosen BaseFunctionHolder
   */
  BaseFunctionHolder getBestMatch(List<BaseFunctionHolder> methods, FunctionCall call);
}
