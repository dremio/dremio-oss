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
package com.dremio.dac.explore.model.extract;

import com.dremio.dac.proto.model.dataset.ExtractCard;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** ExtractCards model */
public class ExtractCards {

  private final List<ExtractCard> cards;

  @JsonCreator
  public ExtractCards(@JsonProperty("cards") List<ExtractCard> cards) {
    this.cards = cards;
  }

  public List<ExtractCard> getCards() {
    return cards;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
