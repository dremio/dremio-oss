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
package com.dremio.exec.physical.config;

import com.dremio.exec.physical.base.AbstractStore;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@Options
@JsonTypeName("screen")
public class Screen extends AbstractStore implements Root {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Screen.class);

  @JsonCreator
  public Screen(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child
      ) {
    super(props, child);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Screen(props, child);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitScreen(this, value);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.SCREEN_VALUE;
  }

}
