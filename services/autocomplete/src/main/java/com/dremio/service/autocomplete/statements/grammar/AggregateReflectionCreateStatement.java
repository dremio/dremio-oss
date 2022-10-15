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
package com.dremio.service.autocomplete.statements.grammar;

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DIMENSIONS;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.MEASURES;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 ALTER TABLE tblname
 ADD AGGREGATE REFLECTION name
 DIMENSIONS (field1, field2)
 MEASURES (field1, field2)
 [ DISTRIBUTE BY (field1, field2, ..) ]
 [ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
 [ LOCALSORT BY (field1, field2, ..) ]
 */
public final class AggregateReflectionCreateStatement extends Statement {
  private final TableReference tableReference;
  private final String name;
  private final FieldList dimensions;
  private final FieldList measures;
  private final FieldLists fieldLists;

  private AggregateReflectionCreateStatement(
    ImmutableList<DremioToken> tokens,
    TableReference tableReference,
    String name,
    FieldList dimensionTokens,
    FieldList measures,
    FieldLists fieldLists) {
    super(tokens, asListIgnoringNulls(tableReference, dimensionTokens, measures, fieldLists));
    Preconditions.checkNotNull(tableReference);
    Preconditions.checkNotNull(name);
    this.tableReference = tableReference;
    this.name = name;
    this.dimensions = dimensionTokens;
    this.measures = measures;
    this.fieldLists = fieldLists;
  }

  public TableReference getTableReference() {
    return tableReference;
  }

  public String getName() {
    return name;
  }

  public FieldList getDimensions() {
    return dimensions;
  }

  public FieldList getMeasures() {
    return measures;
  }

  public FieldLists getFieldLists() {
    return fieldLists;
  }

  static AggregateReflectionCreateStatement parse(
    TokenBuffer tokenBuffer,
    TableReference tableReference,
    String reflectionName) {
    Preconditions.checkNotNull(tokenBuffer);
    Preconditions.checkNotNull(tableReference);
    Preconditions.checkNotNull(reflectionName);
    return builder(tokenBuffer.toList(), tableReference, reflectionName)
      .dimensions(parseDimensions(tokenBuffer, tableReference))
      .measures(parseMeasures(tokenBuffer, tableReference))
      .fieldLists(FieldLists.parse(tokenBuffer, tableReference))
      .build();
  }

  private static FieldList parseDimensions(TokenBuffer tokenBuffer, TableReference tableReference) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(DIMENSIONS);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return FieldList.parse(tokenBuffer, tableReference);
  }

  private static FieldList parseMeasures(TokenBuffer tokenBuffer, TableReference tableReference) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(MEASURES);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return FieldList.parse(tokenBuffer, tableReference);
  }

  private static DimensionsStep builder(ImmutableList<DremioToken> tokens, TableReference tableReference, String tableName) {
    return new Builder(tokens, tableReference, tableName);
  }

  private interface DimensionsStep {
    MeasuresStep dimensions(FieldList dimensions);
  }

  private interface MeasuresStep {
    FieldListStep measures(FieldList measures);
  }

  private interface FieldListStep {
    Build fieldLists(FieldLists fieldLists);
  }

  private interface Build {
    AggregateReflectionCreateStatement build();
  }

  private static final class Builder implements DimensionsStep, MeasuresStep, FieldListStep, Build{
    private final ImmutableList<DremioToken> tokens;
    private final TableReference tableReference;
    private final String tableName;
    private FieldList dimensions;
    private FieldList measures;
    private FieldLists fieldLists;

    public Builder (ImmutableList<DremioToken> tokens, TableReference tableReference, String tableName) {
      this.tokens = tokens;
      this.tableReference = tableReference;
      this.tableName = tableName;
    }

    @Override
    public Builder dimensions(FieldList dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    @Override
    public Builder measures(FieldList measures) {
      this.measures = measures;
      return this;
    }

    @Override
    public Builder fieldLists(FieldLists fieldLists) {
      this.fieldLists = fieldLists;
      return this;
    }

    @Override
    public AggregateReflectionCreateStatement build() {
      return new AggregateReflectionCreateStatement(
        tokens,
        tableReference,
        tableName,
        dimensions,
        measures,
        fieldLists);
    }
  }
}
