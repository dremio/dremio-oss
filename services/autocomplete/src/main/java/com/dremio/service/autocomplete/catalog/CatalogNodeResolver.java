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
package com.dremio.service.autocomplete.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.Utils;
import com.google.common.collect.ImmutableList;

/**
 * Resolves all the CatalogNodes that are valid children of the partial catalog path given.
 */
public final class CatalogNodeResolver {
  private static final ImmutableList<Integer> TABLE_REFERENCE_START_TOKENS = ImmutableList.<Integer>builder()
    .add(ParserImplConstants.FROM)
    .add(ParserImplConstants.COMMA)
    .add(ParserImplConstants.JOIN)
    .add(ParserImplConstants.APPLY)
    .build();

  private final CatalogNodeReader catalogNodeReader;

  public CatalogNodeResolver(CatalogNodeReader catalogNodeReader) {
    Preconditions.checkNotNull(catalogNodeReader);
    this.catalogNodeReader = catalogNodeReader;
  }

  public List<CatalogNode> resolve(final ImmutableList<DremioToken> tokens) {
    /*
    The grammar for table expressions looks like the following:
    FROM tableExpression

    tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

    So we need to scan backwards to see what context we are in.
     */

    final Optional<Integer> lastTableReferenceStartToken = TABLE_REFERENCE_START_TOKENS
      .stream()
      .map(tableReferenceStartToken -> Utils.lastIndexOf(tokens, tableReferenceStartToken))
      .reduce(Optional.empty(), (maxSoFar, next) -> Utils.max(maxSoFar, next));

    if (!lastTableReferenceStartToken.isPresent()) {
      return Collections.EMPTY_LIST;
    }

    ImmutableList<DremioToken> pathTokens = tokens.subList(lastTableReferenceStartToken.get() + 1, tokens.size());
    // We should have a list of the form ["root", ".", "child1", "."]
    // And now we need to find all children of "child1".

    List<String> stringPathTokens = new ArrayList<>();
    for (DremioToken token : pathTokens) {
      if ((token.getKind() != ParserImplConstants.DOT) && (token.getKind() != ParserImplConstants.DOUBLE_QUOTE)) {
        stringPathTokens.add(token.getImage());
      }
    }

    Optional<CatalogNode> nodeAtPath = catalogNodeReader.tryGetCatalogNodeAtPath(stringPathTokens);
    if (!nodeAtPath.isPresent()) {
      return Collections.EMPTY_LIST;
    }

    if (!(nodeAtPath.get() instanceof CatalogNodeWithChildren)) {
      return Collections.EMPTY_LIST;
    }

    CatalogNodeWithChildren nodeWithChildren = (CatalogNodeWithChildren) nodeAtPath.get();
    return nodeWithChildren
      .getChildren()
      .stream()
      .sorted(CatalogNodeComparator.INSTANCE)
      .collect(Collectors.toList());
  }

  private static final class CatalogNodeComparator implements Comparator<CatalogNode> {
    public static final CatalogNodeComparator INSTANCE = new CatalogNodeComparator();

    private CatalogNodeComparator() {}
    @Override
    public int compare(CatalogNode o1, CatalogNode o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
}
