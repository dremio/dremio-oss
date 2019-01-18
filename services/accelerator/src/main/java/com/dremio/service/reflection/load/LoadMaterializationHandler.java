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
package com.dremio.service.reflection.load;

import static com.dremio.service.reflection.ReflectionUtils.getMaterializationPath;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlLoadMaterialization;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Sql syntax handler for the $LOAD MATERIALIZATION METADATA command, an internal command used to refresh materialization metadata.
 */
public class LoadMaterializationHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoadMaterializationHandler.class);

  private final QueryContext context;

  public LoadMaterializationHandler(QueryContext context) {
    this.context = Preconditions.checkNotNull(context, "query context required");
  }

  private static List<String> normalizeComponents(final List<String> components) {
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
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlLoadMaterialization load = SqlNodeUtil.unwrap(sqlNode, SqlLoadMaterialization.class);

    if(!SystemUser.SYSTEM_USERNAME.equals(context.getQueryUserName())) {
      throw SqlExceptionHelper.parseError("$LOAD MATERIALIZATION not supported.", sql, load.getParserPosition()).build(logger);
    }

    final ReflectionService service = Preconditions.checkNotNull(context.getAccelerationManager().unwrap(ReflectionService.class),
      "Couldn't unwrap ReflectionService");

    final List<String> components = normalizeComponents(load.getMaterializationPath());
    if (components == null) {
      throw SqlExceptionHelper.parseError("Invalid materialization path.", sql, load.getParserPosition()).build(logger);
    }

    final ReflectionId reflectionId = new ReflectionId(components.get(0));
    final Optional<ReflectionGoal> goalOptional = service.getGoal(reflectionId);
    if (!goalOptional.isPresent()) {
      throw SqlExceptionHelper.parseError("Unknown reflection id.", sql, load.getParserPosition()).build(logger);
    }
    final ReflectionGoal goal = goalOptional.get();

    final MaterializationId materializationId = new MaterializationId(components.get(1));
    final Optional<Materialization> materializationOpt = service.getMaterialization(materializationId);
    if (!materializationOpt.isPresent()) {
      throw SqlExceptionHelper.parseError("Unknown materialization id.", sql, load.getParserPosition()).build(logger);
    }
    final Materialization materialization = materializationOpt.get();

    // if the user already made changes to the reflection goal, let's stop right here
    Preconditions.checkState(Objects.equals(goal.getTag(), materialization.getReflectionGoalVersion()),
      "materialization no longer matches its goal");

    refreshMetadata(goal, materialization);

    return Collections.singletonList(SimpleCommandResult.successful("Materialization metadata loaded."));
  }

  private void refreshMetadata(final ReflectionGoal goal, final Materialization materialization) {
    final List<ReflectionField> sortedFields =
      Optional.fromNullable(goal.getDetails().getSortFieldList()).or(ImmutableList.<ReflectionField>of());

    final Function<DatasetConfig, DatasetConfig> datasetMutator;
    if (sortedFields.isEmpty()) {
      datasetMutator = null;
    } else {
      datasetMutator = new Function<DatasetConfig, DatasetConfig>() {
        @Override
        public DatasetConfig apply(DatasetConfig datasetConfig) {
          if (datasetConfig.getReadDefinition() == null) {
            logger.warn("Trying to set sortColumnList on a datasetConfig that doesn't contain a read definition");
          } else {
            final List<String> sortColumnsList = FluentIterable.from(sortedFields)
              .transform(new Function<ReflectionField, String>() {
                @Override
                public String apply(ReflectionField field) {
                  return field.getName();
                }
              })
              .toList();
            datasetConfig.getReadDefinition().setSortColumnsList(sortColumnsList);
          }
          return datasetConfig;
        }
      };
    }

    context.getCatalogService()
      .getCatalog(SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build())
        .createDataset(new NamespaceKey(getMaterializationPath(materialization)), datasetMutator);
  }

}
