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
package com.dremio.service.autocomplete.completions;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Completion Item for an autocomplete result.
 */
@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableCompletionItem.class)
@JsonDeserialize(as = ImmutableCompletionItem.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface CompletionItem {
  /**
   * The textual value of the completion item. For example "SELECT".
   * @return
   */
  String getLabel();

  /**
   * The value that should be inserted into the editor.
   * This value may differ from the label.
   * For example the label for a catalog entry could be "this.is.a.space" and the insert text will be "\"this.is.a.space\"".
   * Another example is a column could be "NAME", but the insert text will be "space.folder.table.NAME".
   * @return
   */
  String getInsertText();

  /**
   * The type of completion.
   * @return
   */
  CompletionItemKind getKind();

  /**
   * A description of the completion.
   * @return
   */
  String getDetail();

  /**
   * An object to store some metadata about the completion item.
   */
  Object getData();
}
