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
package com.dremio.dac.service.autocomplete.utils;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.sql.parser.impl.Token;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Type representing a token in a dremio query.
 */
public final class DremioToken {
  public static final DremioToken START_TOKEN = new DremioToken(0, "");

  private final int kind;
  private final String image;

  public DremioToken(int kind, String image) {
    Preconditions.checkNotNull(image);

    this.kind = kind;
    this.image = image;
  }

  public int getKind() {
    return kind;
  }

  public String getImage() {
    return image;
  }

  public static DremioToken fromCalciteToken(Token token) {
    return new DremioToken(token.kind, token.image);
  }

  @Override
  public String toString() {
    return image;
  }

  public static DremioToken createFromParserKind(int parserKind) {
    return ParserTokenCache.INSTANCE.tokens.get(parserKind);
  }

  private static final class ParserTokenCache {
    public static final ParserTokenCache INSTANCE = new ParserTokenCache();

    private final ImmutableList<DremioToken> tokens;

    private ParserTokenCache() {
      ImmutableList.Builder<DremioToken> tokensBuilder = new ImmutableList.Builder<>();
      for (int i = 0; i < ParserImplConstants.tokenImage.length; i++) {
        String normalizedImage = NormalizedTokenDictionary.INSTANCE.indexToImage(i);
        DremioToken dremioToken = new DremioToken(i, normalizedImage);

        tokensBuilder.add(dremioToken);
      }

      tokens = tokensBuilder.build();
    }
  }
}
