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
package com.dremio.exec.store.ischema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ischema.InfoSchemaTable.Catalogs;
import com.dremio.exec.store.ischema.InfoSchemaTable.Columns;
import com.dremio.exec.store.ischema.InfoSchemaTable.Schemata;
import com.dremio.exec.store.ischema.InfoSchemaTable.Tables;
import com.dremio.exec.store.ischema.InfoSchemaTable.Views;
import com.dremio.exec.store.pojo.PojoRecordReader;

/**
 * The set of tables/views in INFORMATION_SCHEMA.
 */
public enum InfoSchemaTableType {
  // TODO:  Resolve how to not have two different place defining table names:
  // NOTE: These identifiers have to match the string values in
  // InfoSchemaConstants.
  CATALOGS(new Catalogs()),
  SCHEMATA(new Schemata()),
  VIEWS(new Views()),
  COLUMNS(new Columns()),
  TABLES(new Tables());

  private final InfoSchemaTable<?> tableDef;

  /**
   * ...
   * @param  tableDef  the definition (columns and data generator) of the table
   */
  InfoSchemaTableType(InfoSchemaTable<?> tableDef) {
    this.tableDef = tableDef;
  }

  public <S> PojoRecordReader<S> getRecordReader(String catalogName, SchemaPlus rootSchema, InfoSchemaFilter filter) {
    @SuppressWarnings("unchecked")
    InfoSchemaRecordGenerator<S> recordGenerator = (InfoSchemaRecordGenerator<S>) tableDef.getRecordGenerator(catalogName);
    recordGenerator.setInfoSchemaFilter(filter);
    recordGenerator.scanSchema(rootSchema);
    return recordGenerator.getRecordReader();
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return tableDef.getRowType(typeFactory);
  }

  public BatchSchema getSchema(){
    return BatchSchema.fromCalciteRowType(getRowType(new JavaTypeFactoryImpl()));
  }
}
