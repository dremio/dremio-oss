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
package com.dremio.dac.explore;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.ExpressionBase;
import com.dremio.dac.explore.model.FromBase.FromVisitor;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromSubQuery;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.google.common.base.Joiner;

/**
 * Manages modifying the state
 */
class DatasetStateMutator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetStateMutator.class);

  private VirtualDatasetState virtualDatasetState;
  private final boolean preview;

  public DatasetStateMutator(String username, VirtualDatasetState virtualDatasetState, boolean preview) {
    super();
    this.virtualDatasetState = ProtostuffUtil.copy(virtualDatasetState);
    this.preview = preview;
  }

  public void setSql(QueryMetadata metadata) {
    virtualDatasetState = QuerySemantics.extract(metadata);
  }

  public void addColumn(int index, Column column) {
    shouldNotExist(column.getName());
    getColumns().add(index, column);
  }

  public void addColumn(Column column) {
    shouldNotExist(column.getName());
    getColumns().add(column);
  }

  public void moveColumn(int index, int dest) {
    List<Column> columns = getColumns();
    Column column = columns.get(index);
    columns.remove(index);
    columns.add(dest - 1, column);
  }

  public int columnCount() {
    return getColumns().size();
  }

  public void addJoin(Join join) {
    List<Join> joins = virtualDatasetState.getJoinsList();
    if (joins != null) {
      joins.add(join);
    } else {
      virtualDatasetState.setJoinsList(asList(join));
    }
  }

  public void updateColumnTables() {
    String alias = getDatasetAlias();
    List<Column> columns = virtualDatasetState.getColumnsList();
    for (Column c : columns) {
      c.getValue().getCol().setTable(alias);
    }
    virtualDatasetState.setColumnsList(columns);
  }

  public String uniqueColumnName(String column) {
    int i = 0;
    String newColumn = column;
    while (findCol(newColumn) != null) {
      newColumn = column + i++;
    }
    return newColumn;
  }

  public String getDatasetAlias() {
    switch (virtualDatasetState.getFrom().getType()) {
    case SubQuery:
      return virtualDatasetState.getFrom().getSubQuery().getAlias();
    case SQL:
      return virtualDatasetState.getFrom().getSql().getAlias();
    case Table:
      if (virtualDatasetState.getFrom().getTable().getAlias() != null) {
        return virtualDatasetState.getFrom().getTable().getAlias();
      } else {
        DatasetPath datasetPath = new DatasetPath(virtualDatasetState.getFrom().getTable().getDatasetPath());
        return datasetPath.getDataset().getName();
      }
    default:
      throw new UnsupportedOperationException("NYI");
    }

  }

  private void shouldNotExist(String colName) {
    if (findCol(colName) != null) {
      throw new IllegalArgumentException(
          format("Invalid new col name %s. It is already in the current schema", colName));
    }
  }

  public void groupedBy(List<Column> newColumns, List<Column> groupBys) {
    List<Order> keepOrders = new ArrayList<>();
    if (groupBys != null) {
      for (Column groupByColumn : groupBys) {
        Ref<Order> order = findOrder(groupByColumn.getName());
        if (order != null) {
          keepOrders.add(order.getVal());
        }
      }
    }
    virtualDatasetState.setOrdersList(keepOrders);
    virtualDatasetState.setColumnsList(newColumns);
    virtualDatasetState.setGroupBysList(groupBys);
  }

  public void addFilter(Filter filter) {
    getFilters().add(filter);
  }

  public void setOrdersList(List<Order> columnsList) {
    virtualDatasetState.setOrdersList(columnsList);
  }

  private List<Column> getColumns() {
    List<Column> columnsList = virtualDatasetState.getColumnsList();
    if (columnsList == null) {
      throw new IllegalStateException("Columns should be populated whenever a dataset is created.");
    }
    return columnsList;
  }

  public TransformResult result() {
    final Set<String> referredTables = new HashSet<>(getReferredTables(virtualDatasetState));
    assignAliases(virtualDatasetState, referredTables);
    return new TransformResult(virtualDatasetState);
  }

  private void assignAliases(VirtualDatasetState state, final Set<String> referredTables) {
    new FromVisitor<Void>() {
      @Override
      public Void visit(FromTable table) throws Exception {
        return null;
      }

      @Override
      public Void visit(FromSQL sql) throws Exception {
        if (sql.getAlias() == null) {
          sql.setAlias(newTableName(referredTables));
        }
        return null;
      }

      @Override
      public Void visit(FromSubQuery subQuery) throws Exception {
        assignAliases(subQuery.getSuqQuery(), referredTables);
        if (subQuery.getAlias() == null) {
          subQuery.setAlias(newTableName(referredTables));
        }
        return null;
      }
    }.visit(state.getFrom());
  }

  public int indexOfCol(String colName) {
    return findColOrFail(colName).getIndex();
  }

  /**
   * @param colName the column name searched
   * @return the column or null if not found
   */
  private Ref<Column> findCol(String colName) {
    List<Column> cols = getColumns();
    for (int i = 0; i < cols.size(); i++) {
      Column column = cols.get(i);
      if (column.getName().equals(colName)) {
        return new Ref<>(column, i);
      }
    }
    return null;
  }

  /**
   * nest the query before transforming
   *
   * For example to apply a group by on an already grouped by query.
   */
  public void nest() {

    // find a new unique table name.
    Set<String> tableNames = getReferredTables(virtualDatasetState);
    String name = newTableName(tableNames);

    VirtualDatasetState newState = new VirtualDatasetState()
        .setFrom(new FromSubQuery(name, virtualDatasetState).wrap());
    List<Column> columns = getColumns();
    List<Column> newColumnsList = new ArrayList<>();
    for (Column column : columns) {
      newColumnsList.add(new Column(column.getName(), new ExpColumnReference(column.getName()).wrap()));
    }

    // update all values for new dataset state.
    newState.setColumnsList(newColumnsList);
    newState.setContextList(virtualDatasetState.getContextList());
    final List<String> tables = new ArrayList<>(tableNames);
    tables.add(name);
    newState.setReferredTablesList(tables);
    virtualDatasetState = newState;
  }

  private static String newTableName(Set<String> tableNames) {
    String name;
    int i = 0;
    do {
      name = "nested_" + i;
      ++i;
    } while (tableNames.contains(name));
    tableNames.add(name);
    return name;
  }

  public TransformResult rename(String oldCol, String newCol) {
    // rename does not nest
    Column col = findCol(oldCol).getVal();
    if (col == null) {
      throw colNotFound(oldCol);
    } else {
      col.setName(newCol);
    }
    Ref<Order> order = findOrder(oldCol);
    if (order != null) {
      order.getVal().setName(newCol);
    }
    return result().modified(newCol);
  }

  public TransformResult apply(String oldCol, String newCol, ExpressionBase newExp, boolean dropSourceColumn) {
    return apply(oldCol, newCol, newExp.wrap(), dropSourceColumn);
  }

  public TransformResult apply(String oldCol, String newCol, Expression newExpWrapped, boolean dropSourceColumn) {
    boolean sameColumn = oldCol.equals(newCol);
    if (sameColumn && !dropSourceColumn) {
      // you can only set the same name if you also drop the old column
      throw UserException.validationError()
        .message("You cannot use a column name that already exists in the table: %s", newCol)
        .build(logger);
    }
    // if preview we just mark the column as deleted
    boolean drop = dropSourceColumn && !preview;
    Ref<Column> colRef = findColForModification(oldCol, drop);
    // if we drop it, we take its place. Otherwise we add the new one right after
    int nextIndex = drop ? colRef.getIndex() : colRef.getIndex() + 1;
    // still want to show the new col even if it has the same name
    String newColName = sameColumn && preview ? newCol + " (new)" : newCol;

    addColumn(nextIndex, new Column(newColName, newExpWrapped));

    TransformResult result = result().added(newColName);
    if (dropSourceColumn) {
      result = result.removed(oldCol);
    }
    return result;
  }

  /**
   * Will find the column by name and return the expression defining it's value
   * @see DatasetStateMutator#findColForModification(String)
   * @param colName the name of the column
   * @return it's value
   * @throws IllegalArgumentException if the column is not found
   */
  public Expression findColValueForModification(String colName) {
    return findColForModification(colName, false).getVal().getValue();
  }

  /**
   * Will find the column by name and return the expression defining it's value
   * assume we are not going to modify it
   * @param colName the name of the column
   * @return it's value
   * @throws IllegalArgumentException if the column is not found
   */
  public Expression findColValue(String colName) {
    return findColOrFail(colName).getVal().getValue();
  }

  /**
   * Will find the column by name and return it
   * As the goal is to modify this column as part of a transformation,
   * if the column is referred in a group by or a sort it will be wrapped in a subquery
   * before returning a value allowing to modify it without changing these operations.
   * @param colName the name of the column
   * @param drop
   * @return the column found by this name
   * @throws IllegalArgumentException if the column is not found
   */
  private Ref<Column> findColForModification(String colName, boolean drop) {
    if (isGroupedBy(colName) || isOrderedBy(colName)) {
      nest();
    }
    Ref<Column> col = findColOrFail(colName);
    if (drop) {
      virtualDatasetState.getColumnsList().remove(col.getIndex());
    }
    return col;
  }

  private Ref<Column> findColOrFail(String colName) {
    Ref<Column> col = findCol(colName);
    if (col == null) {
      throw colNotFound(colName);
    }
    return col;
  }

  private boolean isOrderedBy(String colName) {
    return findOrder(colName) != null;
  }

  private boolean isGroupedBy(String colName) {
    List<Column> groupBys = virtualDatasetState.getGroupBysList();
    if (groupBys != null) {
      for (Column column : groupBys) {
        if (column.getName().equals(colName)) {
          return true;
        }
      }
    }
    return false;
  }

  private List<Filter> getFilters() {
    List<Filter> filters = virtualDatasetState.getFiltersList();
    if (filters == null) {
      filters = new ArrayList<>();
      virtualDatasetState.setFiltersList(filters);
    }
    return filters;
  }

  private IllegalArgumentException colNotFound(String columnName) {
    List<String> colNames = new ArrayList<>();
    for (Column col : getColumns()) {
      colNames.add(col.getName());
    }
    return new IllegalArgumentException(
        format("Invalid col name %s. It is not in the current schema: %s", columnName, Joiner.on(", ").join(colNames)));
  }

  private static Set<String> getReferredTables(final VirtualDatasetState virtualDatasetState) {
    List<String> referred = virtualDatasetState.getReferredTablesList();
    if(referred == null){
      return new HashSet<>();
    } else {
      return new HashSet<>(virtualDatasetState.getReferredTablesList());
    }
  }

  public boolean isGrouped() {
    List<Column> groupBys = virtualDatasetState.getGroupBysList();
    return groupBys != null && !groupBys.isEmpty();
  }

  public void dropColumn(String droppedColumnName) {
    findColForModification(droppedColumnName, true);
    if (getColumns().size() == 0) {
      throw new IllegalArgumentException(format("Can not remove the last column %s", droppedColumnName));
    }

    Ref<Order> order = findOrder(droppedColumnName);
    if (order != null) {
      throw new IllegalArgumentException(format("Dropping columns should not affect order by: %s", droppedColumnName));
    }
  }

  private Ref<Order> findOrder(String col) {
    List<Order> ordersList = virtualDatasetState.getOrdersList();
    if (ordersList != null) {
      for (int i = 0; i < ordersList.size(); i++) {
        Order o = ordersList.get(i);
        if (o.getName().equals(col)) {
          return new Ref<>(o, i);
        }
      }
    }
    return null;
  }

  public void setVirtualDatasetStateReference(List<SourceVersionReference> referencesList) {
    virtualDatasetState.setReferenceList(referencesList);
  }

  private static class Ref<T> {
    private final T val;
    private final int index;
    public Ref(T val, int index) {
      super();
      this.val = val;
      this.index = index;
    }
    public T getVal() {
      return val;
    }
    public int getIndex() {
      return index;
    }
  }

}
