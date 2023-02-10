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
 * A tabstop in the VSCode Snippet:
 *
 * https://code.visualstudio.com/docs/editor/userdefinedsnippets#_tabstops
 *
 * tabstop     ::= '$' int
 */
public final class Tabstop extends SnippetElement {
  private final int index;

  public Tabstop(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "${" + index + "}";
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

    int index = Integer.parseInt(buffer.substring(0, endIndex));

    Tabstop tabstop = new Tabstop(index);
    return Optional.of(tabstop);
  }
}
