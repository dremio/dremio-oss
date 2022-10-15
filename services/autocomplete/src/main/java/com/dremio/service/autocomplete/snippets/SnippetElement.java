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
package com.dremio.service.autocomplete.snippets;

import java.util.Optional;

/**
 * AST for this grammar: https://code.visualstudio.com/docs/editor/userdefinedsnippets#_grammar
 */
public abstract class SnippetElement {
  public static Optional<SnippetElement> tryParse(String text) {
    if (text.isEmpty()) {
      return Optional.empty();
    }

    char firstChar = text.charAt(0);
    if (firstChar != '$') {
      return Text.tryParse(text);
    }

    if (text.length() < 3) {
      return Optional.empty();
    }

    if (text.charAt(1) != '{') {
      return Optional.empty();
    }

    char insideBracketChar = text.charAt(2);
    if (Character.isAlphabetic(insideBracketChar)) {
      return Variable.tryParse(text);
    }

    int colonIndex = text.indexOf(':');
    if (colonIndex > 0) {
      return Placeholder.tryParse(text);
    }

    int pipeIndex = text.indexOf('|');
    if (pipeIndex > 0) {
      return Choice.tryParse(text);
    }

    return Tabstop.tryParse(text);
  }
}
