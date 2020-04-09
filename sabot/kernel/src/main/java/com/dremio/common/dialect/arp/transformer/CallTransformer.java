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

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.common.rel2sql.DremioRelToSqlConverter;

/**
 * Interface which is used to make adjustments to RexCall or SqlCall nodes
 * to make it easier to pass down to ARP.
 */
public abstract class CallTransformer {

  /**
   * Indicates if the given call should be transformed by this.
   */
  public abstract boolean matches(RexCall call);

  /**
   * Indicates if the given operator should be transformed by this.
   */
  public boolean matches(SqlOperator operator) {
    return getCompatibleOperators().contains(operator);
  }

  /**
   * The set of SqlOperators that match this CallTransformer.
   */
  public abstract Set<SqlOperator> getCompatibleOperators();

  /**
   * Transform the operands for a call that matches this transformer.
   */
  public List<SqlNode> transformSqlOperands(List<SqlNode> operands) {
    return operands;
  }

  /**
   * Transform the operands for a call that matches this transformer.
   */
  public List<RexNode> transformRexOperands(List<RexNode> operands) {
    return operands;
  }

  /**
   * Adjust the name for this operator based on its arguments.
   */
  public String adjustNameBasedOnOperands(String operatorName, List<RexNode> operands) {
    return operatorName;
  }

  /**
   * Adjust the SqlOperator for this operator based on its arguments.
   */
  public SqlOperator getAlternateOperator(RexCall call) {
    return call.getOperator();
  }

  /**
   * Returns an alternate operator that is easier to pushdown.
   */
  public Supplier<SqlNode> getAlternateCall(Supplier<SqlNode> originalNodeSupplier,
                                            DremioRelToSqlConverter.DremioContext context, RexProgram program, RexCall call) {
    return originalNodeSupplier;
  }
}
