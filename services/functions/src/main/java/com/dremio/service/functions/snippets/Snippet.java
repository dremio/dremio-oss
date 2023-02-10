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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;

@JsonSerialize(using = SnippetSerializer.class)
@JsonDeserialize(using = SnippetDeserializer.class)
public final class Snippet {
  private final ImmutableList<SnippetElement> snippetElements;

  public Snippet(ImmutableList<SnippetElement> snippetElements) {
    this.snippetElements = snippetElements;
  }

  public ImmutableList<SnippetElement> getSnippetElements() {
    return snippetElements;
  }

  @Override
  public String toString() {
    return String.join("", this.snippetElements
      .stream()
      .map(element -> element.toString())
      .collect(Collectors.toList()));
  }

  public static Optional<Snippet> tryParse(String text) {
    StringBuffer buffer = new StringBuffer(text);
    List<SnippetElement> elements = new ArrayList<>();

    while (buffer.length() != 0) {
      int endIndex = buffer.charAt(0) != '$' ? buffer.indexOf("$") : buffer.indexOf("}") + 1;
      if (endIndex < 0) {
        endIndex = buffer.length();
      }

      String elementString = buffer.substring(0, endIndex);
      Optional<SnippetElement> optionalElement = SnippetElement.tryParse(elementString);
      if (!optionalElement.isPresent()) {
        return Optional.empty();
      }

      SnippetElement element = optionalElement.get();
      elements.add(element);

      buffer.delete(0, element.toString().length());
    }

    Snippet snippet = new Snippet(ImmutableList.copyOf(elements));
    return Optional.of(snippet);
  }

  public static Builder builder() { return new Builder(); }

  public static final class Builder {
    private final List<SnippetElement> snippetElements;
    private int index;

    public Builder() {
      this.snippetElements = new ArrayList<>();
      this.index = 1;
    }

    public Builder addTabstop() {
      Tabstop tabstop = new Tabstop(index++);
      this.snippetElements.add(tabstop);
      return this;
    }

    public Builder addPlaceholder(String placeholderName) {
      Placeholder placeholder = new Placeholder(index++, new Text(placeholderName));
      this.snippetElements.add(placeholder);
      return this;
    }

    public Builder addChoice(String choice1, String choice2, String... choices) {
      Choice choice = new Choice(
        index++,
        ImmutableList.<String>builder()
          .add(choice1, choice2)
          .addAll(Arrays.stream(choices).collect(Collectors.toList()))
          .build());
      this.snippetElements.add(choice);
      return this;
    }

    public Builder addVariable(Variable.Type type) {
      Variable variable = new Variable(type);
      this.snippetElements.add(variable);
      return this;
    }

    public Builder addText(String value) {
      Text text = new Text(value);
      this.snippetElements.add(text);
      return this;
    }

    public Snippet build() {
      return new Snippet(ImmutableList.copyOf(this.snippetElements));
    }
  }
}
