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
package com.dremio.plugins.elastic.planning.rules;

import java.util.Calendar;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDigestIncludeType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.planning.functions.ElasticFunction;
import com.dremio.plugins.elastic.planning.functions.ElasticFunctions;
import com.dremio.plugins.elastic.planning.functions.FunctionRender;
import com.dremio.plugins.elastic.planning.functions.FunctionRenderer;
import com.dremio.plugins.elastic.planning.functions.FunctionRenderer.RenderMode;
import com.dremio.plugins.elastic.planning.rules.SchemaField.ElasticFieldReference;
import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public final class ProjectAnalyzer extends RexVisitorImpl<FunctionRender> {

  private static final Logger logger = LoggerFactory.getLogger(ProjectAnalyzer.class);

  private boolean foundMetaColumn = false;
  private boolean requiresScripts = false;
  private final boolean isAggregationContext;
  private final boolean allowPushdownAnalyzedNormalizedFields;

  private FunctionRenderer renderer;

  public static Script getScript(RexNode node,
      boolean painlessAllowed,
      boolean supportsV5Features,
      boolean scriptsEnabled,
      boolean isAggregationContext,
      boolean allowPushdownAnalyzedNormalizedFields,
      boolean variationDetected){
    ProjectAnalyzer analyzer = new ProjectAnalyzer(isAggregationContext, allowPushdownAnalyzedNormalizedFields);
    RenderMode mode = painlessAllowed && supportsV5Features ? RenderMode.PAINLESS : RenderMode.GROOVY;
    FunctionRenderer r = new FunctionRenderer(supportsV5Features, scriptsEnabled, mode, analyzer);
    analyzer.renderer = r;
    FunctionRender render = node.accept(analyzer);
    if(analyzer.isNotAllowed()){
      throw new RuntimeException(String.format("Failed to convert expression %s to script.", node));
    }

    String nullGuardedScript;
    if (isAggregationContext) {
      nullGuardedScript = render.getNullGuardedScript(variationDetected);
    } else {
      nullGuardedScript = render.getNullGuardedScript("false", variationDetected);
    }

    if (mode == RenderMode.PAINLESS) {
      // when returning a painless script, let's make sure we cast to a valid output type.
      return new Script(ScriptType.INLINE,  "painless", String.format("(def) (%s)", nullGuardedScript), ImmutableMap.of());
    } else {
      // keeping this so plan matching tests will pass
      return new Script(ScriptType.INLINE, "groovy", nullGuardedScript, ImmutableMap.of());
    }
  }

  private ProjectAnalyzer(boolean isAggregationContext, boolean allowPushdownAnalyzedNormalizedFields) {
    super(true);
    this.isAggregationContext = isAggregationContext;
    this.allowPushdownAnalyzedNormalizedFields = allowPushdownAnalyzedNormalizedFields;
  }

  public boolean isNotAllowed() {
    return (requiresScripts && foundMetaColumn);
  }

  @Override
  public FunctionRender visitLiteral(RexLiteral literal) {
    String litVal = literal.computeDigest(RexDigestIncludeType.NO_TYPE);
    if (!renderer.isScriptsEnabled()) {
      throw UserException.permissionError().message("Scripts must be enabled to allow for complex expression pushdowns.").build(logger);
    }
    requiresScripts = true;
    switch (literal.getType().getSqlTypeName()) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        throw UserException.unsupportedError().message("Intervals are not allowed for complex expression pushdowns.").build(logger);

      case BIGINT:
        return new FunctionRender(litVal + "L", ImmutableList.<NullReference>of());

      case DOUBLE:
        return new FunctionRender(litVal + "D", ImmutableList.<NullReference>of());

      case DATE:
      case TIME:
      case TIMESTAMP:
        return new FunctionRender("Instant.ofEpochMilli(" + Long.toString(((Calendar) literal.getValue()).getTimeInMillis()) + "L)", ImmutableList.of());
      default:
        return new FunctionRender(litVal, ImmutableList.<NullReference>of());
    }
  }

  @Override
  public FunctionRender visitInputRef(RexInputRef inputRef) {
    final SchemaField field = (SchemaField) inputRef;
    final SchemaPath path = field.getPath();

    if (ElasticsearchConstants.META_COLUMNS.contains(path.getRootSegment().getPath())) {
      foundMetaColumn = true;
    }

    ElasticFieldReference reference = field.toReference(true, isAggregationContext, allowPushdownAnalyzedNormalizedFields);
    return new FunctionRender(reference.getReference(), reference.getPossibleNulls());
  }

  @Override
  public FunctionRender visitCall(RexCall call) {
    final String funcName = call.getOperator().getName().toLowerCase();

    final ElasticFunction elasticFunction = ElasticFunctions.getFunction(call);

    if (elasticFunction == null) {
      throw new RuntimeException("Unknown function, " + funcName + ", encountered while trying to pushdown to elasticsearch.");
    }

    if (!renderer.isScriptsEnabled()) {
      throw UserException.permissionError().message("Scripts must be enabled to allow for complex expression pushdowns.").build(logger);
    }

    requiresScripts = true;
    FunctionRender render = elasticFunction.render(renderer, call);
    return render;
  }

  @Override
  public FunctionRender visitDynamicParam(RexDynamicParam dynamicParam) {
    return visitUnknown(dynamicParam);
  }

  @Override
  public FunctionRender visitRangeRef(RexRangeRef rangeRef) {
    return visitUnknown(rangeRef);
  }

  @Override
  public FunctionRender visitFieldAccess(RexFieldAccess fieldAccess) {
    return visitUnknown(fieldAccess);
  }

  @Override
  public FunctionRender visitLocalRef(RexLocalRef localRef) {
    return visitUnknown(localRef);
  }

  @Override
  public FunctionRender visitOver(RexOver over) {
    return visitUnknown(over);
  }

  @Override
  public FunctionRender visitCorrelVariable(RexCorrelVariable correlVariable) {
    return visitUnknown(correlVariable);
  }

  protected FunctionRender visitUnknown(RexNode o){
    // raise an error
    throw UserException.planError()
            .message("Unsupported for elastic pushdown: RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString())
            .build(logger);
  }
}
