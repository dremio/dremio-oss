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
package com.dremio.service.autocomplete.functions;

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.*;

import java.util.Optional;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Parse Tree for a Function.
 * This enables users to determine what tokens belong to which parameters in a function call.
 * For example if we have a function like:
 *
 * FOO(A, B + C, BAR(D))
 *
 * Then the parameter tokens are
 *  1) A
 *  2) B + C
 *  3) BAR(D)
 *
 * We could also have a function like:
 *
 * SUBSTRING(A FROM B + C FOR BAR(D))
 *
 * And we need to again parse out the same parameters.
 *
 * From here we can see what this class is trying to accomplish.
 * We should be able to parse out the parameters regardless of the syntax of the function.
 */
public final class ParsedFunction {
  private final String name;
  private final ImmutableList<ParsedParameter> parameters;

  private static final ImmutableSet<Integer> fromComma = ImmutableSet.of(COMMA, FROM);

  private static final ImmutableSet<Integer> forComma = ImmutableSet.of(COMMA, FOR);

  private static final ImmutableSet<Integer> allDistinct = ImmutableSet.of(ALL, DISTINCT);

  private ParsedFunction(
    String name,
    ImmutableList<ParsedParameter> parameters) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(parameters);
    this.name = name;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public ImmutableList<ParsedParameter> getParameters() {
    return parameters;
  }

  public static Optional<ParsedFunction> tryParse(ImmutableList<DremioToken> tokens) {
    return tryParse( TokenBuffer.create(tokens));
  }

  public static Optional<ParsedFunction> tryParse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    String name = tokenBuffer.readImage();
    if (tokenBuffer.isEmpty()) {
      return Optional.empty();
    }

    if (tokenBuffer.readIfKind(LPAREN) == null) {
      return Optional.empty();
    }

    ImmutableList<DremioToken> parameterTokens = getParameterTokens(tokenBuffer.toList());
    if (parameterTokens.isEmpty()) {
      return Optional.of(new ParsedFunction(name, ImmutableList.of()));
    }

    TokenBuffer parameterTokensBuffer = new TokenBuffer(parameterTokens);

    ImmutableList<ParsedParameter> parameters;
    switch (name.toUpperCase()) {
      case "SUBSTRING":
        parameters = parseSubstringParameters(parameterTokensBuffer);
        break;

      case "ANY_VALUE":
      case "AVG":
      case "BIT_AND":
      case "BIT_OR":
      case "BIT_XOR":
      case "COLLECT":
      case "LISTAGG":
      case "MAX":
      case "MIN":
      case "STDDEV":
      case "STDDEV_POP":
      case "STDDEV_SAMP":
      case "SUM":
      case "VAR_POP":
      case "VAR_SAMP":
        parameters = parseAggregateAllOrDistinctParameters(parameterTokensBuffer);
        break;

      case "COUNT":
        parameters = parseCountParameters(parameterTokensBuffer);
        break;

      default:
        parameters = parseCommaSeparatedParameters(parameterTokensBuffer);
        break;
    }

    ParsedFunction parsedFunction = new ParsedFunction(name, parameters);

    return Optional.of(parsedFunction);
  }

  private static ImmutableList<DremioToken> getParameterTokens(ImmutableList<DremioToken> remainingTokens) {
    // Find the matching right parens
    int level = 1;
    int stopIndex;
    for (stopIndex = 0; (stopIndex < remainingTokens.size()) && (level != 0); stopIndex++) {
      DremioToken token = remainingTokens.get(stopIndex);
      switch (token.getKind()) {
      case LPAREN:
        level++;
        break;
      case RPAREN:
        level--;
        break;
      default:
        // DO NOTHING
      }
    }

    ImmutableList<DremioToken> parameterTokens = remainingTokens.subList(0, level == 0 ? stopIndex - 1 : stopIndex);
    return parameterTokens;
  }

  private static ImmutableList<ParsedParameter> parseCommaSeparatedParameters(
    TokenBuffer tokenBuffer) {
    ImmutableList.Builder<ParsedParameter> parsedParameterBuilder = new ImmutableList.Builder<>();
    while (!tokenBuffer.isEmpty()) {
      ImmutableList<DremioToken> parameterTokens = tokenBuffer.readUntilMatchKindAtSameLevel(COMMA);
      tokenBuffer.readIfKind(COMMA);
      ParsedParameter parsedParameter = ParsedParameter.parse(parameterTokens);
      parsedParameterBuilder.add(parsedParameter);
    }

    return parsedParameterBuilder.build();
  }

  private static ImmutableList<ParsedParameter> parseSubstringParameters(
    TokenBuffer tokenBuffer) {
    /**
     * SUBSTRING(string FROM integer)
     * SUBSTRING(string FROM integer FOR integer)
     * SUBSTRING(<STRING>, <INTEGER>)
     * SUBSTRING(<STRING>, <INTEGER>, <INTEGER>)
     */
    ImmutableList.Builder<ParsedParameter> parsedParameterBuilder = new ImmutableList.Builder<>();
    ImmutableList<DremioToken> stringParameterTokens = tokenBuffer.readUntilMatchKindsAtSameLevel(fromComma);
    ParsedParameter stringParameter = ParsedParameter.parse(stringParameterTokens);
    parsedParameterBuilder.add(stringParameter);
    tokenBuffer.readIfKinds(fromComma);
    if (tokenBuffer.isEmpty()) {
      return parsedParameterBuilder.build();
    }

    ImmutableList<DremioToken> fromParameterTokens = tokenBuffer.readUntilMatchKindsAtSameLevel(forComma);
    ParsedParameter fromParameter = ParsedParameter.parse(fromParameterTokens);
    parsedParameterBuilder.add(fromParameter);
    tokenBuffer.readIfKinds(forComma);
    if (tokenBuffer.isEmpty()) {
      return parsedParameterBuilder.build();
    }

    ImmutableList<DremioToken> forParameterTokens = tokenBuffer.drainRemainingTokens();
    ParsedParameter forParameter = ParsedParameter.parse(forParameterTokens);
    parsedParameterBuilder.add(forParameter);

    return parsedParameterBuilder.build();
  }

  private static ImmutableList<ParsedParameter> parseAggregateAllOrDistinctParameters(
    TokenBuffer tokenBuffer) {
    tokenBuffer.readIfKinds(allDistinct);
    return parseCommaSeparatedParameters(tokenBuffer);
  }

  private static ImmutableList<ParsedParameter> parseCountParameters(
    TokenBuffer tokenBuffer) {
    if (tokenBuffer.peekKind() == STAR) {
      return ImmutableList.of();
    }
    return parseAggregateAllOrDistinctParameters(tokenBuffer);
  }
}
