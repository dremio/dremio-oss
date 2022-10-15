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

import com.google.common.collect.ImmutableList;

/**
 * A choice in the VSCode Snippet:
 *
 * https://code.visualstudio.com/docs/editor/userdefinedsnippets#_choice
 *
 * choice      ::= '${' int '|' text (',' text)* '|}'
 */
public final class Choice extends SnippetElement {
  private final int index;
  private final ImmutableList<String> choices;

  public Choice(int index, ImmutableList<String> choices) {
    this.index = index;
    this.choices = choices;
  }

  public int getIndex() {
    return index;
  }

  public ImmutableList<String> getChoices() {
    return choices;
  }

  @Override
  public String toString() {
    return "${" + this.index + "|" + String.join(",", this.choices) + "|}";
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

    int pipeIndex = buffer.indexOf("|");
    if (pipeIndex < 0) {
      return Optional.empty();
    }

    int index = Integer.parseInt(buffer.substring(0, pipeIndex));

    buffer.delete(0, pipeIndex + 1);
    if (buffer.length() == 0) {
      return Optional.empty();
    }

    pipeIndex = buffer.indexOf("|");
    if (pipeIndex < 0) {
      return Optional.empty();
    }

    String choicesString = buffer.substring(0, pipeIndex);
    ImmutableList<String> choices = ImmutableList.copyOf(choicesString.split(","));

    buffer.delete(0, pipeIndex + 1);
    if (buffer.length() == 0) {
      return Optional.empty();
    }

    if (buffer.charAt(0) != '}') {
      return Optional.empty();
    }

    Choice choice = new Choice(index, choices);
    return Optional.of(choice);
  }
}
