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
package com.dremio.service.functions.snippets;

import java.util.Optional;

/**
 * A variable in the VSCode Snippet:
 *
 * <p>https://code.visualstudio.com/docs/editor/userdefinedsnippets#_variables
 *
 * <p>variable ::= '$' var
 */
public final class Variable extends SnippetElement {
  private final Type type;

  public Variable(Type type) {
    this.type = type;
  }

  public Type getType() {
    return this.type;
  }

  @Override
  public String toString() {
    return "${" + this.type + "}";
  }

  public enum Type {
    TM_SELECTED_TEXT,
    TM_CURRENT_LINE,
    TM_CURRENT_WORD,
    TM_LINE_INDEX,
    TM_LINE_NUMBER,
    TM_FILENAME,
    TM_FILENAME_BASE,
    TM_DIRECTORY,
    TM_FILEPATH,
    RELATIVE_FILEPATH,
    CLIPBOARD,
    WORKSPACE_NAME,
    WORKSPACE_FOLDER,
    CURSOR_INDEX,
    CURSOR_NUMBER,

    CURRENT_YEAR,
    CURRENT_YEAR_SHORT,
    CURRENT_MONTH,
    CURRENT_MONTH_NAME,
    CURRENT_MONTH_NAME_SHORT,
    CURRENT_DATE,
    CURRENT_DAY_NAME,
    CURRENT_DAY_NAME_SHORT,
    CURRENT_HOUR,
    CURRENT_MINUTE,
    CURRENT_SECOND,
    CURRENT_SECONDS_UNIX,

    RANDOM,
    RANDOM_HEX,
    UUID,

    BLOCK_COMMENT_START,
    BLOCK_COMMENT_END,
    LINE_COMMENT
  }

  public static Optional<SnippetElement> tryParse(String text) {
    if (text.isEmpty()) {
      return Optional.empty();
    }

    StringBuffer buffer = new StringBuffer(text);
    if (buffer.charAt(0) != '$') {
      return Optional.empty();
    }

    buffer.deleteCharAt(0);
    if (buffer.length() == 0) {
      return Optional.empty();
    }

    if (buffer.charAt(0) != '{') {
      return Optional.empty();
    }

    buffer.deleteCharAt(0);
    if (buffer.length() == 0) {
      return Optional.empty();
    }

    int endIndex = buffer.indexOf("}");
    if (endIndex < 0) {
      return Optional.empty();
    }

    Type type;
    try {
      type = Type.valueOf(buffer.substring(0, endIndex));
    } catch (Exception ex) {
      return Optional.empty();
    }

    Variable variable = new Variable(type);
    return Optional.of(variable);
  }
}
