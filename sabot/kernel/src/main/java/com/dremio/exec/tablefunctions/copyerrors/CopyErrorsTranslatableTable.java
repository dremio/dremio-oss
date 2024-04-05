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
package com.dremio.exec.tablefunctions.copyerrors;

import com.dremio.exec.planner.sql.handlers.query.CopyErrorContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Triple;

/** Result table for copy_errors macro. */
public final class CopyErrorsTranslatableTable implements TranslatableTable {
  private final CopyErrorContext context;
  public static final List<Triple<String, SqlTypeName, ArrowType>>
      COPY_ERRORS_TABLEFUNCTION_SCHEMA =
          new ArrayList() {
            {
              add(Triple.of("job_id", SqlTypeName.VARCHAR, ArrowType.Utf8.INSTANCE));
              add(Triple.of("file_name", SqlTypeName.VARCHAR, ArrowType.Utf8.INSTANCE));
              add(Triple.of("line_number", SqlTypeName.BIGINT, new ArrowType.Int(64, true)));
              add(Triple.of("row_number", SqlTypeName.BIGINT, new ArrowType.Int(64, true)));
              add(Triple.of("column_name", SqlTypeName.VARCHAR, ArrowType.Utf8.INSTANCE));
              add(Triple.of("error", SqlTypeName.VARCHAR, ArrowType.Utf8.INSTANCE));
            }
          };

  public CopyErrorsTranslatableTable(CopyErrorContext context) {
    this.context = context;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new CopyErrorsCrel(
        context.getCluster(),
        context.getCluster().traitSet(),
        this.context,
        new CopyErrorsCatalogMetadata(
            getBatchSchema(), getRowType(context.getCluster().getTypeFactory())));
  }

  private static BatchSchema getBatchSchema() {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    COPY_ERRORS_TABLEFUNCTION_SCHEMA.stream()
        .map(p -> Field.nullable(p.getLeft(), p.getRight()))
        .forEach(schemaBuilder::addField);
    BatchSchema schema = schemaBuilder.build();
    return schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    COPY_ERRORS_TABLEFUNCTION_SCHEMA.forEach(p -> builder.add(p.getLeft(), p.getMiddle()));
    return builder.build();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }
}
