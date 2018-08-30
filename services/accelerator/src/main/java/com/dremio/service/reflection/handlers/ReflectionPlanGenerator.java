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
package com.dremio.service.reflection.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.planner.acceleration.normalization.Normalizer;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

/**
 * Encapsulates all the logic needed to generate a reflection's plan
 */
public class ReflectionPlanGenerator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionPlanGenerator.class);

  private final NamespaceService namespaceService;
  private final OptionManager optionManager;
  private final SabotConfig config;
  private final SqlHandlerConfig sqlHandlerConfig;

  public ReflectionPlanGenerator(
      SqlHandlerConfig sqlHandlerConfig,
      NamespaceService namespaceService,
      OptionManager optionManager,
      SabotConfig config) {
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
    this.config = Preconditions.checkNotNull(config, "sabot config required");
    this.sqlHandlerConfig = Preconditions.checkNotNull(sqlHandlerConfig, "SqlHandlerConfig required.");
  }

  public RelNode generateNormalizedPlan(final ReflectionGoal goal, final RelTransformer relTransformer) {
    // retrieve reflection's dataset
    final DatasetConfig dataset = namespaceService.findDatasetByUUID(goal.getDatasetId());
    if (dataset == null) {
      throw new IllegalStateException(String.format("reflection %s has no corresponding dataset", ReflectionUtils.getId(goal)));
    }
    // generate dataset's plan and viewFieldTypes
    final NamespaceKey path = new NamespaceKey(dataset.getFullPathList());

    return getPlan(path, relTransformer);
  }


  /**
   * SqlParserPos pos,
      SqlNodeList keywordList,
      SqlNodeList selectList,
      SqlNode from,
      SqlNode where,
      SqlNodeList groupBy,
      SqlNode having,
      SqlNodeList windowDecls,
      SqlNodeList orderBy,
      SqlNode offset,
      SqlNode fetch
   * @param path
   * @return
   * @throws ValidationException
   * @throws RelConversionException
   * @throws ForemanSetupException
   */
  private RelNode getPlan(final NamespaceKey path, RelTransformer relTransformer) {
    SqlSelect select = new SqlSelect(
        SqlParserPos.ZERO,
        new SqlNodeList(SqlParserPos.ZERO),
        new SqlNodeList(ImmutableList.<SqlNode>of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
        new SqlIdentifier(path.getPathComponents(), SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        null,
        null,
        null
        );

    try {
      ConvertedRelNode converted = PrelTransformer.validateAndConvert(sqlHandlerConfig, select, relTransformer);

      return converted.getConvertedNode();
    } catch (ForemanSetupException | RelConversionException | ValidationException e) {
      throw Throwables.propagate(SqlExceptionHelper.coerceException(logger, select.toString(), e, false));
    }
  }

  /**
   * A normalizer class doing nothing
   */
  public static final class PassThruNormalizer implements Normalizer {

    @Override
    public RelNode normalize(RelNode query) {
      return query;
    }
  }
}
