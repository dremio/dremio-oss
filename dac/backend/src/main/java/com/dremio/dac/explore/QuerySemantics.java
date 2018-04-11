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
package com.dremio.dac.explore;

import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.dac.proto.model.dataset.OrderDirection.DESC;
import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.exec.calcite.SqlNodes;
import com.dremio.exec.planner.sql.BaseSqlVisitor;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.google.common.base.Optional;

/**
 * Allows us to convert user submitted queries into internal dataset state to provide better queries upon transformation.
 */
public class QuerySemantics {
  private static final Logger logger = LoggerFactory.getLogger(QuerySemantics.class);

  public QuerySemantics(){}

  /**
   * Will parse a sql query and return a Dataset state
   * @param query the SQL to parse
   * @return the corresponding state
   */
  public static VirtualDatasetState extract(SqlQuery query, QueryMetadata metadata) {
    return new QuerySemantics().instanceExtract(query, metadata);
  }

  public VirtualDatasetState instanceExtract(SqlQuery query, QueryMetadata metadata) {
    RelDataType relDataType = metadata.getRowType();
    VirtualDatasetState state;
    try {
      state = extract(query.getSql(), metadata.getSqlNode().get(), relDataType);
    } catch (RuntimeException e) {
      state = fallback("Error parsing", null, query.getSql(), e);
    }
    if (state.getColumnsList() == null || state.getColumnsList().isEmpty()) {
      populateSemanticFields(relDataType, state);
    }
    Optional<List<String>> referredTables = metadata.getReferredTables();
    if (referredTables.isPresent()) {
      state.setReferredTablesList(referredTables.get());
    }
    state.setContextList(query.getContext());
    return state;

  }

  private VirtualDatasetState fallback(String message, SqlNode node, String sql) {
    return fallback(message, node, sql, null);
  }

  private VirtualDatasetState fallback(String message, SqlNode node, String sql, Throwable t) {
    return fallback(message + (node == null ? "" : " on node:\n" + SqlNodes.toTreeString(node)), sql, t);
  }

  protected VirtualDatasetState fallback(String message, String sql, Throwable t) {
    // When we don't understand the query we just fall back to wrapping
    logger.debug(message + "\nfalling back to wrapping:\n" + sql, t);
    return new VirtualDatasetState()
        .setFrom(new FromSQL(sql).setAlias("nested_0").wrap());
  }

  private VirtualDatasetState extract(String sql, SqlNode node, final RelDataType relDataType) {
    final VirtualDatasetState state;
    switch (node.getKind()) {
    case SELECT:
      state = extractSelect(sql, node, relDataType);
      break;
    case ORDER_BY:
      state = extractOrderBy(sql, node, relDataType);
      break;
    default:
      state = fallback("Unknown kind of call " + node.getKind(), node, sql);
      break;
    }
    return state;
  }

  public static void populateSemanticFields(RelDataType relDataType, VirtualDatasetState state){
    List<String> columnNames = relDataType.getFieldNames();
    List<Column> columns = new ArrayList<>();
    for (String colName : columnNames) {
      ExpColumnReference colRef = new ExpColumnReference(colName);
      columns.add(new Column(colName, colRef.wrap()));
    }
    state.setColumnsList(columns);
  }

  private VirtualDatasetState extractOrderBy(String sql, SqlNode node, final RelDataType relDataType) {
    SqlOrderBy orderBy = (SqlOrderBy)node;
    return extract(sql, orderBy.query, relDataType);
  }

  private VirtualDatasetState extractSelect(String sql, SqlNode node, final RelDataType relDataType) {
    SqlSelect select = (SqlSelect)node;

    // From table
    final SqlNode fromNode = select.getFrom();
    if (fromNode == null) {
      return fallback("without FROM clause", node, sql);
    }
    final FromNode from = extractFrom(fromNode);

    // Selected columns
    List<Column> columns = extractColumns(relDataType, select, from);

    SqlNode where = select.getWhere();
    if (where != null) {
      return fallback("where is not supported yet", where, sql);
    }
    SqlNodeList groupBy = select.getGroup();
    if (groupBy != null) {
      return fallback("group by is not supported yet", groupBy, sql);
    }
    SqlNode having = select.getHaving();
    if (having != null) {
      return fallback("having is not supported yet", having, sql);
    }
    SqlNodeList windowDecls = select.getWindowList();
    if (windowDecls != null && !windowDecls.getList().isEmpty()) {
      return fallback("window is not supported yet", windowDecls, sql);
    }
    List<Order> orders = extractOrders(select.getOrderList(), from);

    SqlNode offset = select.getOffset();
    if (offset != null) {
      return fallback("offset is not supported yet", offset, sql);
    }
    SqlNode fetch = select.getFetch();
    if (fetch != null) {
      return fallback("fetch is not supported yet", fetch, sql);
    }
    FromTable fromTable = new FromTable(from.getTableToString());
    if (from.alias != null) {
      fromTable.setAlias(from.getAliasToString());
    }
    return new VirtualDatasetState()
        .setFrom(fromTable.wrap())
        .setColumnsList(columns)
        .setOrdersList(orders);
  }

  private List<Order> extractOrders(SqlNodeList orderBy, final FromNode from) {
    List<Order> orders = new ArrayList<>();
    if (orderBy != null) {
      for (SqlNode sqlNode : orderBy.getList()) {
        orders.add(sqlNode.accept(new BaseSqlVisitor<Order>() {
          @Override
          public Order visit(SqlIdentifier id) {
            return new Order(idToRef(from, id), ASC);
          }
          @Override
          public Order visit(SqlCall call) {
            switch (call.getOperator().getKind()) {
            // there's no ASCENDING. It always fall in the id case above
            case DESCENDING:
              List<SqlNode> operandList = call.getOperandList();
              if (operandList.size() != 1) {
                throw new UnsupportedOperationException("Unexpected DESC operands in order clause:\n" + SqlNodes.toTreeString(call));
              }
              SqlNode operand = operandList.get(0);
              if (operand.getKind() == IDENTIFIER) {
                return new Order(idToRef(from, (SqlIdentifier)operand), DESC);
              } else {
                throw new UnsupportedOperationException("Unexpected DESC operand in order clause:\n" + SqlNodes.toTreeString(call));
              }
            default:
              throw new UnsupportedOperationException("Unexpected SqlOperatorImpl in order clause:\n" + SqlNodes.toTreeString(call));
            }
          }
        }));
      }
    }
    if (orders.size() == 0) {
      return null;
    }
    return orders;
  }

  private FromNode extractFrom(final SqlNode from) {
    return from.accept(new BaseSqlVisitor<FromNode>() {
      @Override
      public FromNode visit(SqlIdentifier id) {
        return new FromNode(null, id);
      }
      @Override
      public FromNode visit(SqlCall call) {
        SqlOperator operator = call.getOperator();
        switch (operator.getKind()) {
        case AS:
          ASNode as = extractAS(call);
          if (as.exp.getKind() == IDENTIFIER) {
            SqlIdentifier ref = (SqlIdentifier)as.exp;
            if (!as.alias.isSimple()) {
              throw new UnsupportedOperationException("Table aliasing not supported:\n" + SqlNodes.toTreeString(call));
            }
            return new FromNode(as.alias, ref);
          } else {
            throw new UnsupportedOperationException("Unexpected AS:\n" + SqlNodes.toTreeString(call));
          }
        default:
          throw new UnsupportedOperationException("Unexpected operator in call: " + operator.getKind() + "\n" + SqlNodes.toTreeString(call));
        }
      }
    });
  }

  private List<Column> extractColumns(final RelDataType relDataType, SqlSelect select, final FromNode from) {
    List<SqlNode> selectList = select.getSelectList().getList();
    // If the select list is * query, return null for column list
    if (selectList.size() == 1) {
      final SqlNode sqlNode = selectList.get(0);
      if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier)sqlNode).isStar()) {
        return null;
      }
    }

    // If the select list contains a '*', we can't extract columns as the relDataType will have '*' expanded
    for(SqlNode sqlNode : selectList) {
      if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier)sqlNode).isStar()) {
        throw new UnsupportedOperationException("* with other fields\n" + SqlNodes.toTreeString(select.getSelectList()));
      }
    }

    List<Column> columns = new ArrayList<>(selectList.size());

    List<RelDataTypeField> fieldList = relDataType.getFieldList();
    if (fieldList.size() != selectList.size()) {
      throw new UnsupportedOperationException("col size mismatch with type: " + relDataType +"\n" +SqlNodes.toSQLString(select.getSelectList()));
    }
    for (int i = 0; i < fieldList.size(); i++) {
      final RelDataTypeField field = fieldList.get(i);
      SqlNode sqlNode = selectList.get(i);
      Column c = sqlNode.accept(new BaseSqlVisitor<Column>(){
        @Override public Column visit(SqlIdentifier id) {
          return new Column(field.getName(), new ExpColumnReference(idToRef(from, id)).wrap());
        }

        @Override public Column visit(SqlCall call) {
          SqlOperator operator = call.getOperator();
          switch (operator.getKind()) {
          case AS:
            ASNode as = extractAS(call);
            if (!field.getName().equals(as.getAliasToString())) {
              throw new UnsupportedOperationException(
                  "Unexpected AS field name in call: " + field.getName() + " != " + as.getAliasToString() + " \n" +
                      SqlNodes.toTreeString(call));
            }
            Expression exp = as.exp.getKind() == IDENTIFIER ?
                new ExpColumnReference(idToRef(from, (SqlIdentifier)as.exp)).wrap()
                : new ExpCalculatedField(SqlNodes.toSQLString(as.exp)).wrap();
            return new Column(field.getName(), exp);
          default:
            return new Column(field.getName(), new ExpCalculatedField(SqlNodes.toSQLString(call)).wrap());
          }
        }
      });
      columns.add(c);
    }

    return columns;
  }

  private static boolean isSimpleID(SqlNode node) {
    return node.getKind() == IDENTIFIER && ((SqlIdentifier)node).isSimple();
  }

  private static String idToRef(FromNode from, SqlIdentifier id) {
    if (isSimpleID(id)) {
      // TODO: need to check if quotes are correct
      return id.getSimple();
    } else {
      SqlIdentifier finalId = id;
      // removing unnecessary table prefix from col ref.
      if (id.names.size() > from.alias.names.size()) {
        boolean isPrefix = true;
        for (int i = 0; i < from.alias.names.size(); i++) {
          String an = from.alias.names.get(i);
          String idn = id.names.get(i);
          if (!an.equals(idn)) {
            isPrefix = false;
            break;
          }
        }
        if (isPrefix) {
          finalId = id.getComponent(from.alias.names.size(), id.names.size());
        }
      }
      if (!finalId.isSimple()) {
        throw new IllegalArgumentException("expected a simple type column name (directly or prefixed with table name/alias)");
      }
      return finalId.getSimple();
    }
  }

  private static class FromNode {
    private final SqlIdentifier alias;
    private final SqlIdentifier table;
    public FromNode(SqlIdentifier alias, SqlIdentifier table) {
      this.alias = alias;
      this.table = table;
      if (alias != null && !alias.isSimple()) {
        throw new IllegalArgumentException("alias must be a simple id: " + alias);
      }
    }
    public String getTableToString() {
      return SqlNodes.toSQLString(table);
    }
    public String getAliasToString() {
      return alias.getSimple();
    }
  }

  private static class ASNode {
    private final SqlIdentifier alias;
    private final SqlNode exp;
    public ASNode(SqlIdentifier alias, SqlNode exp) {
      this.alias = alias;
      this.exp = exp;
      if (alias != null && !alias.isSimple()) {
        throw new IllegalArgumentException("alias must be a simple id: " + alias);
      }
    }
    public String getAliasToString() {
      return alias.getSimple();
    }
  }

  private static ASNode extractAS(SqlCall call) {
    if (call.getOperator().getKind() == SqlKind.AS) {
      List<SqlNode> operandList = call.getOperandList();
      if (operandList.size() == 2) {
        SqlNode exp = operandList.get(0);
        SqlNode colID = operandList.get(1);
        if (isSimpleID(colID)) {
          return new ASNode((SqlIdentifier)colID, exp);
        } else {
          throw new UnsupportedOperationException("Unexpected AS " + colID + "\n" + SqlNodes.toTreeString(call));
        }
      } else {
        throw new UnsupportedOperationException("Unexpected AS operands in field: \n" + SqlNodes.toTreeString(call));
      }
    }
    throw new UnsupportedOperationException("AS not understood: " + SqlNodes.toSQLString(call));
  }
}
