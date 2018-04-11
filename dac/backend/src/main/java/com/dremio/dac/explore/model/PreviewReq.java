/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Card preview request
 *
 * @param <R> the rule implementation
 * @param <S> selection
 */
public class PreviewReq<R, S> {
  // TODO: we don't use the selection, just the column name in selection. Not removing now to avoid destabilising UI
  // and e2e tests.
  private final S selection;
  private final R rule;
  @JsonCreator
  public PreviewReq(
      @JsonProperty("selection") S selection,
      @JsonProperty("rule") R rule) {
    super();
    this.selection = selection;
    this.rule = rule;
  }
  public S getSelection() {
    return selection;
  }
  public R getRule() {
    return rule;
  }
}

