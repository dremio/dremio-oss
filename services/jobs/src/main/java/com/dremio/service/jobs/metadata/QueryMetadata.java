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
package com.dremio.service.jobs.metadata;

import static com.dremio.common.utils.Protos.listNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A description of information we use to better understand a query.
 */
public class QueryMetadata {

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryMetadata.class);

  private static final Set<String> RESERVED_PARENT_NAMES = ImmutableSet.of("dremio_limited_preview");

  private final RelDataType rowType;
  private final Optional<List<SqlIdentifier>> ancestors;
  private final Optional<List<FieldOrigin>> fieldOrigins;
  private final Optional<List<JoinInfo>> joins;
  private final Optional<List<ParentDatasetInfo>> parents;
  private final Optional<SqlNode> sqlNode;
  private final Optional<List<ParentDataset>> grandParents;
  private final Optional<RelOptCost> cost;
  private final Optional<PlanningSet> planningSet;
  private final Optional<RelNode> serializableLogicalPlan;
  private final BatchSchema batchSchema;

  public QueryMetadata(List<SqlIdentifier> ancestors,
                       List<FieldOrigin> fieldOrigins, List<JoinInfo> joins, List<ParentDatasetInfo> parents,
                       SqlNode sqlNode, RelDataType rowType,
                       List<ParentDataset> grandParents, final RelOptCost cost, final PlanningSet planningSet,
                       final RelNode serializableLogicalPlan,
                       BatchSchema batchSchema) {
    this.rowType = rowType;

    this.ancestors = Optional.fromNullable(ancestors);
    this.fieldOrigins = Optional.fromNullable(fieldOrigins);
    this.joins = Optional.fromNullable(joins);
    this.parents = Optional.fromNullable(parents);
    this.sqlNode = Optional.fromNullable(sqlNode);
    this.grandParents = Optional.fromNullable(grandParents);
    this.cost = Optional.fromNullable(cost);
    this.planningSet = Optional.fromNullable(planningSet);
    this.serializableLogicalPlan = Optional.fromNullable(serializableLogicalPlan);
    this.batchSchema = batchSchema;
  }

  public Optional<List<String>> getReferredTables() {
    if (!ancestors.isPresent()) {
      return Optional.absent();
    }
    Set<String> tableNames = new HashSet<>();
    for (SqlIdentifier id : ancestors.get()) {
      if (id.names.size() > 0) {
        tableNames.add(id.names.get(id.names.size() - 1));
      }
    }
    return Optional.<List<String>>of(new ArrayList<>(tableNames));
  }

  public Optional<List<ParentDataset>> getGrandParents() {
    return grandParents;
  }

  public Optional<SqlNode> getSqlNode() {
    return sqlNode;
  }

  public Optional<List<SqlIdentifier>> getAncestors() {
    return ancestors;
  }

  public Optional<List<FieldOrigin>> getFieldOrigins() {
    return fieldOrigins;
  }

  public Optional<List<JoinInfo>> getJoins() {
    return joins;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Optional<List<ParentDatasetInfo>> getParents() {
    return parents;
  }

  public Optional<RelNode> getSerializableLogicalPlan() {
    return serializableLogicalPlan;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  /**
   * Returns original cost of query past logical planning.
   */
  public Optional<RelOptCost> getCost() {
    return cost;
  }

  public Optional<PlanningSet> getPlanningSet() {
    return planningSet;
  }

  /**
   * Create a builder for QueryMetadata.
   * @param namespace A namespace service. If provided, ParentDatasetInfo will be extracted, otherwise it won't.
   * @return The builder.
   */
  public static Builder builder(NamespaceService namespace){
    return new Builder(namespace);
  }

  /**
   * A builder to construct query metadata.
   */
  public static class Builder {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Builder.class);

    private final NamespaceService namespace;
    private RelDataType rowType;
    private RelNode logical;
    private RelNode prejoin;
    private SqlNode sql;
    private RelOptCost cost;
    private PlanningSet planningSet;
    private RelNode serializablePlan;
    private BatchSchema batchSchema;

    Builder(NamespaceService namespace){
      this.namespace = namespace;
    }

    public Builder addRowType(RelDataType rowType){
      this.rowType = rowType;
      return this;
    }

    public Builder addLogicalPlan(RelNode rel) {
      this.logical = rel;
      return this;
    }

    public Builder addBatchSchema(BatchSchema schema) {
      this.batchSchema = schema;
      return this;
    }

    public Builder addPreJoinPlan(RelNode rel) {
      this.prejoin = rel;
      return this;
    }

    public Builder addParsedSql(SqlNode sql) {
      this.sql = sql;
      return this;
    }

    public Builder addCost(final RelOptCost cost) {
      this.cost = cost;
      return this;
    }

    public Builder addSerializablePlan(final RelNode serializablePlan) {
      this.serializablePlan = serializablePlan;
      return this;
    }

    /**
     * Sets parallelized query plan.
     */
    public Builder setPlanningSet(final PlanningSet planningSet) {
      this.planningSet = planningSet;
      return this;
    }

    public QueryMetadata build() throws ValidationException {
      Preconditions.checkNotNull(rowType, "The validated row type must be observed before reporting metadata.");

      List<SqlIdentifier> ancestors = null;
      if (sql != null) {
        ancestors = Lists.newArrayList(
            Iterables.filter(
                AncestorsVisitor.extractAncestors(sql),
                new Predicate<SqlIdentifier>() {
                  @Override
                  public boolean apply(@Nullable SqlIdentifier input) {
                    return !RESERVED_PARENT_NAMES.contains(input.toString());
                  }
                }
            )
        );
      }

      List<JoinInfo> joins = null;
      if (logical != null) {
        joins = JoinExtractor.getJoins(logical);
        if (prejoin != null && (joins == null || joins.isEmpty())) {
          joins = JoinExtractor.getJoins(prejoin);
        }
      }

      List<FieldOrigin> fieldOrigins = null;
      if (logical != null && rowType != null) {
        try {
          fieldOrigins = ImmutableList.copyOf(FieldOriginExtractor.getFieldOrigins(logical, rowType));
        } catch (Exception e) {
          // If we fail to extract the column origins, don't fail the query
          logger.debug("Failed to extract column origins for query: " + sql);
        }
      }

      // Make sure there are no duplicate column names
      SqlHandlerUtil.validateRowType(true, Lists.<String>newArrayList(), rowType);



      return new QueryMetadata(
        ancestors, // list of parents
        fieldOrigins,
        joins,
        getParentsFromSql(ancestors), // convert parent to ParentDatasetInfo
        sql,
        rowType,
        getGrandParents(ancestors), // list of all parents to be stored with dataset
        cost, // query cost past logical
        planningSet,
        serializablePlan,
        batchSchema
      );
    }

    /**
     * Return list of all parents for given dataset
     * @param parents parents of dataset from sql.
     * @throws NamespaceException
     */
    private List<ParentDataset> getGrandParents(List<SqlIdentifier> parents) {
      if (parents == null) {
        return null;
      }

      final Map<NamespaceKey, Integer> parentsToLevelMap = Maps.newHashMap();
      final List<NamespaceKey> parentKeys = Lists.newArrayList();
      final List<ParentDataset> grandParents = Lists.newArrayList();

      for (SqlIdentifier parent : parents) {
        final NamespaceKey parentKey = new NamespaceKey(parent.names);
        parentsToLevelMap.put(parentKey, 1);
        parentKeys.add(parentKey);
      }

      try {
        // add parents of parents.
        if (!parentKeys.isEmpty()) {
          for (NameSpaceContainer container : namespace.getEntities(parentKeys)) {
            if (container != null && container.getType() == Type.DATASET) { // missing parent
              if (container.getDataset() != null) {
                final VirtualDataset virtualDataset = container.getDataset().getVirtualDataset();
                if (virtualDataset != null) {
                  if (virtualDataset.getParentsList() != null) {
                    // add parents of parents
                    for (ParentDataset parentDataset : virtualDataset.getParentsList()) {
                      final NamespaceKey parentKey = new NamespaceKey(parentDataset.getDatasetPathList());
                      if (!parentsToLevelMap.containsKey(parentKey)) {
                        parentsToLevelMap.put(parentKey, parentDataset.getLevel() + 1);
                      }
                    }
                    // add grand parents of parent too
                    if (virtualDataset.getGrandParentsList() != null) {
                      for (ParentDataset grandParentDataset : virtualDataset.getGrandParentsList()) {
                        final NamespaceKey parentKey = new NamespaceKey(grandParentDataset.getDatasetPathList());
                        if (!parentsToLevelMap.containsKey(parentKey)) {
                          parentsToLevelMap.put(parentKey, grandParentDataset.getLevel() + 1);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } catch (NamespaceException ne) {
        logger.error("Failed to get list of grand parents", ne);
      }

      for (Map.Entry<NamespaceKey, Integer> entry : parentsToLevelMap.entrySet()) {
        if (entry.getValue() > 1) {
          grandParents.add(new ParentDataset().setDatasetPathList(entry.getKey().getPathComponents()).setLevel(entry.getValue()));
        }
      }
      return grandParents;
    }

    /**
     * Return lists of {@link ParentDatasetInfo} from given list of directly referred tables in the query.
     * @return The list of directly referenced virtual or physical datasets
     */
    private List<ParentDatasetInfo> getParentsFromSql(List<SqlIdentifier> ancestors) {
      if (ancestors == null) {
        return null;
      }
      try {
        final List<ParentDatasetInfo> result = new ArrayList<>();
        for (SqlIdentifier sqlIdentifier : ancestors) {
          final NamespaceKey datasetPath = new NamespaceKey(sqlIdentifier.names);
          result.add(getDataset(datasetPath));
        }
        return result;
      } catch (RuntimeException e) {
        logger.warn(
            "Failure while attempting to extract parents from dataset. This is likely due to  "
            + "a datasource no longer being available that was used in a past job.", e);
        return Collections.emptyList();
      }
    }

    private ParentDatasetInfo getDataset(NamespaceKey path) {
      // fallback
      String rootEntityName = path.getRoot();
      List<String> cleanedPathComponents = Lists.newArrayList();

      if (rootEntityName.indexOf(PathUtils.getPathDelimiter()) > -1) {
        final List<String> spacePathComponents = PathUtils.parseFullPath(path.getRoot());
        cleanedPathComponents.addAll(spacePathComponents);
        List<String> pathComponents = path.getPathComponents();
        for (String folderName : pathComponents.subList(1, pathComponents.size())) {
          cleanedPathComponents.add(folderName);
        }
        rootEntityName = spacePathComponents.get(0);
      } else {
        cleanedPathComponents.addAll(path.getPathComponents());
      }

      // try the original path and then try the cleaned path.
      for(List<String> paths : Arrays.asList(path.getPathComponents(), cleanedPathComponents )) {
        try {
          List<NameSpaceContainer> containers = namespace.getEntities(Collections.singletonList(new NamespaceKey(paths)));
          if (!containers.isEmpty()) {
            final NameSpaceContainer container = containers.get(0);
            if(container != null && container.getType() == Type.DATASET){
              DatasetConfig config = container.getDataset();
              return new ParentDatasetInfo()
                  .setDatasetPathList(config.getFullPathList())
                  .setType(config.getType());
            }
          }
        } catch(NamespaceException | IllegalArgumentException e) {
          // Ignore
        }
      }



      return new ParentDatasetInfo().setDatasetPathList(cleanedPathComponents);
    }
  }

  public static List<String> getSources(DatasetConfig datasetConfig) {
    final Set<String> sources = Sets.newHashSet();
    if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
      if (datasetConfig.getVirtualDataset().getFieldOriginsList() != null) {
        for (FieldOrigin fieldOrigin : datasetConfig.getVirtualDataset().getFieldOriginsList()) {
          for (Origin origin : listNotNull(fieldOrigin.getOriginsList())) {
            sources.add(origin.getTableList().get(0));
          }
        }
      }
    } else {
      sources.add(datasetConfig.getFullPathList().get(0));
    }
    return new ArrayList<>(sources);
  }
}
