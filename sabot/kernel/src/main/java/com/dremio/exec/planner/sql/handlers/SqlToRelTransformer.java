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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.ValuesRewriteShuttle;
import com.dremio.exec.planner.normalizer.NormalizerException;
import com.dremio.exec.planner.normalizer.RelNormalizerTransformer;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.ReflectionHints;
import com.dremio.exec.planner.sql.ReflectionHintsExtractor;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.sql.UnsupportedQueryPlanVisitor;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.DremioHint;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.planner.sql.parser.UnsupportedOperatorsVisitor;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.options.OptionValue;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlToRelTransformer {
  public static final Logger LOGGER = LoggerFactory.getLogger(SqlToRelTransformer.class);

  public static RelNode preprocessNode(RelNode rel) throws SqlUnsupportedException {
    /*
     * Traverse the tree to do the following pre-processing tasks:
     *
     * 1) Replace the convert_from, convert_to function to
     * actual implementations Eg: convert_from(EXPR, 'JSON') be converted to convert_fromjson(EXPR);
     * TODO: Ideally all function rewrites would move here instead of RexToExpr.
     *
     * 2) See where the tree contains unsupported functions; throw SqlUnsupportedException if there is any.
     *
     * 3) Rewrite LogicalValue's row type and replace any Decimal tuples with double
     * since we don't support decimal type during execution.
     * See com.dremio.exec.planner.logical.ValuesRel.writeLiteral where we write Decimal as Double.
     * See com.dremio.exec.vector.complex.fn.VectorOutput.innerRun where we throw exception for Decimal type.
     */

    PreProcessRel visitor = PreProcessRel.createVisitor(rel.getCluster().getRexBuilder());
    try {
      rel = rel.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }

    rel = ValuesRewriteShuttle.rewrite(rel);

    return rel;
  }

  public static ConvertedRelNode validateAndConvert(SqlHandlerConfig config, SqlNode sqlNode)
      throws ForemanSetupException, RelConversionException, ValidationException {
    return validateAndConvert(config, sqlNode, createSqlValidatorAndToRelContext(config));
  }

  public static ConvertedRelNode validateAndConvertForDml(
      SqlHandlerConfig config, SqlNode sqlNode, String sourceName)
      throws ForemanSetupException, ValidationException {
    SqlValidatorAndToRelContext.Builder builder =
        config
            .getConverter()
            .getExpansionSqlValidatorAndToRelContextBuilderFactory()
            .builder()
            .requireSubqueryExpansion(); // This is required because generate rex sub-queries does
    // not work update/delete/insert
    // We do not need to build with version context if we do not have version context in our sql.
    if (sqlNode instanceof SqlDmlOperator) {
      SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) sqlNode;
      TableVersionContext versionContextFromSql = DmlUtils.getVersionContext(sqlDmlOperator);
      if (versionContextFromSql != TableVersionContext.NOT_SPECIFIED
          && versionContextFromSql.getType() != TableVersionType.NOT_SPECIFIED) {
        builder = builder.withVersionContext(sourceName, versionContextFromSql);
      }
    }
    return validateAndConvert(config, sqlNode, builder.build());
  }

  public static ConvertedRelNode validateAndConvertForReflectionRefreshAndCompact(
      SqlHandlerConfig config, SqlNode sqlNode, RelTransformer relTransformer)
      throws ForemanSetupException,
          RelConversionException,
          ValidationException,
          NormalizerException {
    SqlValidatorAndToRelContext sqlValidatorAndToRelContext =
        createSqlValidatorAndToRelContext(config);
    final RelNormalizerTransformer relNormalizerTransformer = config.getRelNormalizerTransformer();
    final AttemptObserver observer = config.getObserver();

    ConvertedRelNode convertedRelNode =
        validateThenSqlToRel(config, sqlNode, sqlValidatorAndToRelContext);

    final RelNode rel =
        relNormalizerTransformer.transformForReflection(
            convertedRelNode.getConvertedNode(), relTransformer, observer);

    return new ConvertedRelNode.Builder()
        .withRelNode(rel)
        .withValidatedRowType(convertedRelNode.getValidatedRowType())
        .build();
  }

  private static ConvertedRelNode validateThenSqlToRel(
      SqlHandlerConfig config,
      SqlNode sqlNode,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext)
      throws ForemanSetupException, ValidationException {

    final Pair<SqlNode, RelDataType> validatedTypedSqlNode =
        validateNode(config, sqlValidatorAndToRelContext, sqlNode);

    config
        .getObserver()
        .beginState(AttemptObserver.toEvent(UserBitShared.AttemptEvent.State.PLANNING));

    final RelNode relNode =
        convertSqlToRel(config, sqlValidatorAndToRelContext, validatedTypedSqlNode.getKey());
    UnsupportedQueryPlanVisitor.checkForUnsupportedQueryPlan(relNode);
    List<NamespaceKey> viewIdentifiers = collectViewIdentifiers(relNode);
    processReflectionHints(config, relNode);

    return new ConvertedRelNode.Builder()
        .withRelNode(relNode)
        .withValidatedRowType(validatedTypedSqlNode.getValue())
        .withViewIdentifiers(viewIdentifiers)
        .build();
  }

  private static ConvertedRelNode validateAndConvert(
      SqlHandlerConfig config,
      SqlNode sqlNode,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext)
      throws ForemanSetupException, ValidationException {
    final RelNormalizerTransformer relNormalizerTransformer = config.getRelNormalizerTransformer();
    final AttemptObserver observer = config.getObserver();

    ConvertedRelNode convertedRelNode =
        validateThenSqlToRel(config, sqlNode, sqlValidatorAndToRelContext);

    final RelNode rel =
        relNormalizerTransformer.transform(convertedRelNode.getConvertedNode(), observer);

    return new ConvertedRelNode.Builder()
        .withRelNode(rel)
        .withValidatedRowType(convertedRelNode.getValidatedRowType())
        .withViewIdentifiers(convertedRelNode.getViewIdentifiers())
        .build();
  }

  private static SqlValidatorAndToRelContext createSqlValidatorAndToRelContext(
      SqlHandlerConfig config) {
    return config
        .getConverter()
        .getUserQuerySqlValidatorAndToRelContextBuilderFactory()
        .builder()
        .build();
  }

  private static Pair<SqlNode, RelDataType> validateNode(
      SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode sqlNode)
      throws ValidationException, ForemanSetupException {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final SqlNode sqlNodeValidated;

    try {
      sqlNodeValidated = sqlValidatorAndToRelContext.validate(sqlNode);
    } catch (final Throwable ex) {
      throw new ValidationException("unable to validate sql node", ex);
    }
    final Pair<SqlNode, RelDataType> typedSqlNode =
        new Pair<>(
            sqlNodeValidated,
            sqlValidatorAndToRelContext.getValidator().getValidatedNodeType(sqlNodeValidated));

    // Check if the unsupported functionality is used
    UnsupportedOperatorsVisitor visitor =
        UnsupportedOperatorsVisitor.createVisitor(config.getContext());
    try {
      sqlNodeValidated.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      // If the exception due to the unsupported functionalities
      visitor.convertException();

      // If it is not, let this exception move forward to higher logic
      throw ex;
    }

    config
        .getObserver()
        .planValidated(
            typedSqlNode.getValue(),
            typedSqlNode.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS),
            config
                .getMaterializations()
                .map(MaterializationList::isMaterializationCacheInitialized)
                .orElse(true));
    return typedSqlNode;
  }

  private static RelNode convertSqlToRel(
      SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode validatedNode) {
    final AttemptObserver observer = config.getObserver();

    final Stopwatch stopwatch = Stopwatch.createStarted();

    final RelRoot convertible =
        sqlValidatorAndToRelContext.toConvertibleRelRoot(validatedNode, true);
    observer.planConvertedToRel(convertible.rel, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    return convertible.rel;
  }

  private static void processReflectionHints(SqlHandlerConfig config, RelNode relRaw) {
    Pointer<ReflectionHints> reflectionHintsPointer = new Pointer<>(null);
    relRaw.accept(
        new StatelessRelShuttleImpl() {
          private int depth = 0;

          @Override
          public RelNode visit(LogicalProject project) {
            if (reflectionHintsPointer.value != null) {
              return project; // Once hints are found, don't recursively look for more
            }

            if (!project.getHints().isEmpty()) {
              reflectionHintsPointer.value = ReflectionHintsExtractor.extract(project);
            }

            return super.visit(project);
          }

          @Override
          public RelNode visit(RelNode other) {
            // Allow hints from either query or first top-level view
            if (other instanceof ExpansionNode) {
              if (depth == 1) {
                return other;
              }
              try {
                depth++;
                return super.visit(other);
              } finally {
                depth--;
              }
            }
            return this.visitChildren(other);
          }
        });

    if (reflectionHintsPointer.value == null) {
      return;
    }

    ReflectionHints reflectionHints = reflectionHintsPointer.value;

    if (reflectionHints.getOptionalConsiderReflections().isPresent()) {
      Set<String> considerReflections = reflectionHints.getOptionalConsiderReflections().get();
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createString(
                  OptionValue.OptionType.QUERY,
                  DremioHint.CONSIDER_REFLECTIONS.getOption().getOptionName(),
                  String.join(",", considerReflections)));
    }

    if (reflectionHints.getOptionalExcludeReflections().isPresent()) {
      Set<String> excludeReflections = reflectionHints.getOptionalExcludeReflections().get();
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createString(
                  OptionValue.OptionType.QUERY,
                  DremioHint.EXCLUDE_REFLECTIONS.getOption().getOptionName(),
                  String.join(",", excludeReflections)));
    }
    if (reflectionHints.getOptionalChooseIfMatched().isPresent()) {
      Set<String> chooseIfMatched = reflectionHints.getOptionalChooseIfMatched().get();
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createString(
                  OptionValue.OptionType.QUERY,
                  DremioHint.CHOOSE_REFLECTIONS.getOption().getOptionName(),
                  String.join(",", chooseIfMatched)));
    }

    if (reflectionHints.getOptionalNoReflections().isPresent()) {
      boolean noReflections = reflectionHints.getOptionalNoReflections().get();
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  DremioHint.NO_REFLECTIONS.getOption().getOptionName(),
                  noReflections));
    }

    if (reflectionHints.getOptionalCurrentIcebergDataOnly().isPresent()) {
      boolean currentIcebergDataOnly = reflectionHints.getOptionalCurrentIcebergDataOnly().get();
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  DremioHint.CURRENT_ICEBERG_DATA_ONLY.getOption().getOptionName(),
                  currentIcebergDataOnly));
    }
  }

  private static List<NamespaceKey> collectViewIdentifiers(RelNode relNode) {
    List<NamespaceKey> viewIdentifiers = new ArrayList<>();
    relNode.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(RelNode other) {
            if (other instanceof ExpansionNode) {
              ExpansionNode e = (ExpansionNode) other;
              viewIdentifiers.add(e.getPath());
            }
            return this.visitChildren(other);
          }
        });
    return viewIdentifiers;
  }
}
