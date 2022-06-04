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
package com.dremio.service.autocomplete;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.planner.sql.parser.impl.Token;

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
}
