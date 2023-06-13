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
package com.dremio.common.dialect.arp.transformer;

import java.util.Set;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;

public class NoOpTransformer extends CallTransformer {

  public static final NoOpTransformer INSTANCE = new NoOpTransformer();

  private NoOpTransformer() {

  }
  /**
   * The set of SqlOperators that match this CallTransformer.
   */
  @Override
  public Set<SqlOperator> getCompatibleOperators() {
    throw new UnsupportedOperationException("NoOpTransformer matches every operator");
  }

  @Override
  public boolean matches(RexCall call) {
    return true;
  }

  @Override
  public boolean matches(SqlOperator op) {
    return true;
  }
}
