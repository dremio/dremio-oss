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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.BY;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.CONSOLIDATED;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DISTRIBUTE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LOCALSORT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.PARTITION;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.STRIPED;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;

/**
 * Stores the fields that are common to both an aggregate and raw reflection create statement.
 */
public final class FieldLists {
  private final FieldList distributeFields;
  private final FieldList partitionFields;
  private final FieldList localSortFields;

  public FieldLists(
    FieldList distributeFields,
    FieldList partitionFields,
    FieldList localSortFields) {
    this.distributeFields = distributeFields;
    this.partitionFields = partitionFields;
    this.localSortFields = localSortFields;
  }

  public FieldList getDistributeFields() {
    return distributeFields;
  }

  public FieldList getPartitionFields() {
    return partitionFields;
  }

  public FieldList getLocalSortFields() {
    return localSortFields;
  }

  public static FieldLists parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return new Builder()
      .addDistributeFields(parseDistributeFields(tokenBuffer))
      .addPartitionFields(parsePartitionFields(tokenBuffer))
      .addLocalSortFields(parseLocalSortFields(tokenBuffer))
      .build();
  }

  private static final class Builder {
    private FieldList distributeFields;
    private FieldList partitionFields;
    private FieldList localSortFields;

    public Builder () {
    }

    public FieldLists.Builder addDistributeFields(FieldList distributeFields) {
      this.distributeFields = distributeFields;
      return this;
    }

    public FieldLists.Builder addPartitionFields(FieldList partitionFields) {
      this.partitionFields = partitionFields;
      return this;
    }

    public FieldLists.Builder addLocalSortFields(FieldList localSortFields) {
      this.localSortFields = localSortFields;
      return this;
    }

    public FieldLists build() {
      return new FieldLists(
        distributeFields,
        partitionFields,
        localSortFields);
    }
  }

  private static FieldList parseDistributeFields(TokenBuffer tokenBuffer) {
    return parseFields(DISTRIBUTE, tokenBuffer);
  }

  private static FieldList parsePartitionFields(TokenBuffer tokenBuffer) {
    tokenBuffer.readIf(token -> (token.getKind() == STRIPED) || (token.getKind() == CONSOLIDATED));
    return parseFields(PARTITION, tokenBuffer);
  }

  private static FieldList parseLocalSortFields(TokenBuffer tokenBuffer) {
    return parseFields(LOCALSORT, tokenBuffer);
  }

  private static FieldList parseFields(int kind, TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    DremioToken firstToken = tokenBuffer.readIfKind(kind);
    if ((firstToken == null) || tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(BY);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return FieldList.parse(tokenBuffer);
  }
}
