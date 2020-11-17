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
package com.dremio.service.reflection.compact;

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.SqlCompactMaterialization;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.reflection.ReflectionGoalChecker;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.WriterOptionManager;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Sql syntax handler for the $COMPACT REFRESH command, an internal command used to compact reflection refreshes.
 */
public class CompactRefreshHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompactRefreshHandler.class);

  private final WriterOptionManager writerOptionManager;

  private String textPlan;

  public CompactRefreshHandler() {
    this.writerOptionManager = WriterOptionManager.Instance;
  }

  private List<String> normalizeComponents(final List<String> components) {
    if (components.size() != 1 && components.size() != 2) {
      return null;
    }

    if (components.size() == 2) {
      return components;
    }

    // there is one component, let's see if we can split it (using only slash paths instead of dotted paths).
    final String[] pieces = components.get(0).split("/");
    if(pieces.length != 2) {
      return null;
    }

    return ImmutableList.of(pieces[0], pieces[1]);
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try {
      final SqlCompactMaterialization compact = SqlNodeUtil.unwrap(sqlNode, SqlCompactMaterialization.class);

      if(!SystemUser.SYSTEM_USERNAME.equals(config.getContext().getQueryUserName())) {
        throw SqlExceptionHelper.parseError("$COMPACT REFRESH not supported.", sql, compact.getParserPosition())
          .build(logger);
      }

      ReflectionService service = config.getContext().getAccelerationManager().unwrap(ReflectionService.class);

      // Let's validate the plan.
      final List<String> materializationPath = normalizeComponents(compact.getMaterializationPath());
      if (materializationPath == null) {
        throw SqlExceptionHelper.parseError("Unknown materialization", sql, compact.getParserPosition())
          .build(logger);
      }

      final ReflectionId reflectionId = new ReflectionId(materializationPath.get(0));
      Optional<ReflectionGoal> goalOpt = service.getGoal(reflectionId);
      if(!goalOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown reflection id.", sql, compact.getParserPosition()).build(logger);
      }
      final ReflectionGoal goal = goalOpt.get();

      Optional<ReflectionEntry> entryOpt = service.getEntry(reflectionId);
      if(!entryOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown reflection id.", sql, compact.getParserPosition()).build(logger);
      }
      final ReflectionEntry entry = entryOpt.get();
      if(!ReflectionGoalChecker.checkGoal(goal, entry)) {
        throw UserException.validationError().message("Reflection has been updated since reflection was scheduled.").build(logger);
      }

      Optional<Materialization> materializationOpt = service.getMaterialization(new MaterializationId(materializationPath.get(1)));
      if (!materializationOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown materialization id", sql, compact.getParserPosition()).build(logger);
      }
      final Materialization materialization = materializationOpt.get();

      List<Refresh> refreshes = Lists.newArrayList(service.getRefreshes(materialization));
      if (refreshes.size() != 1) {
        throw SqlExceptionHelper.parseError("Invalid materialization", sql, compact.getParserPosition()).build(logger);
      }

      Optional<Materialization> newMaterializationOpt = service.getMaterialization(new MaterializationId(compact.getNewMaterializationId()));
      if (!newMaterializationOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown new materialization id", sql, compact.getParserPosition()).build(logger);
      }
      final Materialization newMaterialization = newMaterializationOpt.get();

      final List<String> tableSchemaPath = ReflectionUtils.getMaterializationPath(materialization);

      final PlanNormalizer planNormalizer = new PlanNormalizer(config);
      final RelNode initial = getPlan(config, tableSchemaPath, planNormalizer);

      final Rel drel = PrelTransformer.convertToDrelMaintainingNames(config, initial);
      final List<String> fields = drel.getRowType().getFieldNames();
      final long ringCount = config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT);
      final Rel writerDrel = new WriterRel(
        drel.getCluster(),
        drel.getCluster().traitSet().plus(Rel.LOGICAL),
        drel,
        config.getContext().getCatalog().createNewTable(
          new NamespaceKey(ReflectionUtils.getMaterializationPath(newMaterialization)),
          null,
          writerOptionManager.buildWriterOptionForReflectionGoal((int) ringCount, goal, fields),
          ImmutableMap.of()
        ),
        initial.getRowType()
      );

      final RelNode doubleWriter = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(), config.getContext(), writerDrel);

      final ScreenRel screen = new ScreenRel(writerDrel.getCluster(), writerDrel.getTraitSet(), doubleWriter);

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, screen);
      final Prel prel = convertToPrel.getKey();
      this.textPlan = convertToPrel.getValue();
      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);

      if (logger.isTraceEnabled()) {
        PrelTransformer.log(config, "Dremio Plan", plan, logger);
      }

      return plan;
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  private RelNode getPlan(SqlHandlerConfig sqlHandlerConfig, List<String> refreshTablePath, PlanNormalizer planNormalizer) {
    SqlSelect select = new SqlSelect(
      SqlParserPos.ZERO,
      new SqlNodeList(SqlParserPos.ZERO),
      new SqlNodeList(ImmutableList.<SqlNode>of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
      new SqlIdentifier(refreshTablePath, SqlParserPos.ZERO),
      null,
      null,
      null,
      null,
      null,
      null,
      null
    );

    try {
      ConvertedRelNode converted = PrelTransformer.validateAndConvert(sqlHandlerConfig, select, planNormalizer);

      return converted.getConvertedNode();
    } catch (ForemanSetupException | RelConversionException | ValidationException e) {
      throw Throwables.propagate(SqlExceptionHelper.coerceException(logger, select.toString(), e, false));
    }
  }

  private class PlanNormalizer implements RelTransformer {
    private final OptionManager optionManager;

    PlanNormalizer(SqlHandlerConfig sqlHandlerConfig) {
      this.optionManager = sqlHandlerConfig.getContext().getOptions();
    }

    @Override
    public RelNode transform(RelNode relNode) {
      final String partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
      final Matcher directoryMatcher = Pattern.compile(String.format("%s([0-9]+)", Pattern.quote(partitionDesignator))).matcher("");
      return ReflectionUtils.removeColumns(relNode, (field) ->
        UPDATE_COLUMN.equals(field.getName()) || directoryMatcher.reset(field.getName().toLowerCase()).matches()
      );
    }

  }

}
