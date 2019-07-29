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
package com.dremio.exec.expr;

import java.util.Iterator;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.expr.fn.GandivaFunctionHolderExpression;

/**
 * Dummy expression node to hold the code generation runtime context
 * to decide the execution mode in Gandiva or Java.
 *
 * Needed because of DX-14564.
 */
public class CodeGenContext implements LogicalExpression {

  private CompleteType outputType = null;

  public LogicalExpression getChild() {
    return child;
  }

  LogicalExpression child;
  SupportedEngines executionEngineForChildNode = new SupportedEngines();
  SupportedEngines executionEngineForChildNodeSubExpressions = new SupportedEngines();

  public CodeGenContext(LogicalExpression child) {
    this.child = child;
    // Gandiva only function - can only be executed in Gandiva
    if (child instanceof GandivaFunctionHolderExpression) {
      addSupportedEngine(SupportedEngines.Engine.GANDIVA);
    } else {
      addSupportedEngine(SupportedEngines.Engine.JAVA);
    }
  }

  /**
   * Used to create context free nodes, where support is hand managed.
   * @param child
   */
  public static CodeGenContext buildWithNoDefaultSupport(LogicalExpression child) {
    CodeGenContext context =  new CodeGenContext(child);
    // clear all of the engines.
    context.markSubExprIsMixed();
    context.getExecutionEngineForExpression().clear();
    return context;
  }

  private void addSupportedEngine(SupportedEngines.Engine engine) {
    executionEngineForChildNode.add(engine);
    executionEngineForChildNodeSubExpressions.add(engine);
  }

  public boolean isExpressionExecutableInEngine(SupportedEngines.Engine engine) {
    return executionEngineForChildNode.contains(engine);
  }

  public void addSupportedExecutionEngineForExpression(SupportedEngines.Engine engine) {
    executionEngineForChildNode.add(engine);
  }

  public SupportedEngines getExecutionEngineForExpression() {
    return executionEngineForChildNode;
  }

  public boolean isSubExpressionExecutableInEngine(SupportedEngines.Engine engine) {
    return executionEngineForChildNodeSubExpressions.contains(engine);
  }

  public void addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine engine) {
    executionEngineForChildNodeSubExpressions.add(engine);
  }

  public SupportedEngines getExecutionEngineForSubExpression() {
    return executionEngineForChildNodeSubExpressions;
  }

  public void removeSupporteExecutionEngineForExpression(SupportedEngines.Engine engine) {
    executionEngineForChildNode.remove(engine);
  }

  public void removeSupporteExecutionEngineForSubExpression(SupportedEngines.Engine engine) {
    executionEngineForChildNodeSubExpressions.remove(engine);
  }

  public void markSubExprIsMixed() {
    executionEngineForChildNodeSubExpressions.clear();
  }

  public boolean isMixedModeExecution() {
    return executionEngineForChildNodeSubExpressions.supportedEngines.isEmpty();
  }

  @Override
  public CompleteType getCompleteType() {
    if (outputType != null) {
      return outputType;
    }
    // Output derivation does not expect context nodes; Need to remove all context before attempting
    // the output type derivation.
    LogicalExpression childWithoutContext = CodeGenerationContextRemover.removeCodeGenContext
      (child);
    outputType = childWithoutContext.getCompleteType();
    return outputType;
  }

  public CodeGenContext accept(PreferenceBasedSplitter spiltVisitor,
                               SplitDependencyTracker tracker) throws Exception {
    return spiltVisitor.visitCodeGenContext(this, tracker);
  }

  @Override
  public int getSelfCost() {
    return child.getSelfCost();
  }

  @Override
  public int getCumulativeCost() {
    return child.getCumulativeCost();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return child.iterator();
  }

  public String toString() {
    LogicalExpression childWithoutContext = CodeGenerationContextRemover.removeCodeGenContext
      (child);
    return childWithoutContext.toString();
  }

  @Override
  public int hashCode() {
    return child.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return child.equals(obj);
  }

  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return child.accept(visitor, value);
  }

}
