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
package com.dremio.dac.explore.model;

import com.dremio.dac.proto.model.dataset.DataType;

/**
 * The type of a column or value
 */
public abstract class DataTypes {

  public static String getLabel(DataType type) {
    switch (type) {
    case TEXT : return "Abc";
    case BINARY : return "10";
    case BOOLEAN : return "T|F";
    case FLOAT : return "#.#";
    case INTEGER : return "#";
    case DECIMAL : return "#.#\u0332";
    case MIXED : return "A#";
    case DATE : return "\uD83D\uDCC5";
    case TIME : return "\uD83D\uDD50";
    case DATETIME : return "\uD83D\uDCC5\uD83D\uDD50";
    case LIST : return "\uD83D\uDD32\uD83D\uDD32\uD83D\uDD32";
    case MAP : return "\u22D4";
    case GEO : return "\uD83C\uDF10";
    case OTHER : default: return "?";
    }
  }
}
