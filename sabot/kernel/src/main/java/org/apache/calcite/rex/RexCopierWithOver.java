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
package org.apache.calcite.rex;

/**
 * Rex copier that also handles window over nodes.
 *
 * TODO(DX-14028): Remove once DX-14208 is complete.
 */
public class RexCopierWithOver extends RexCopier {

  private final RexBuilder builder;
  public RexCopierWithOver(RexBuilder builder) {
    super(builder);
    this.builder = builder;
  }

  @Override
  public RexNode visitOver(RexOver over) {
    // this is safe currently since we use a static type factory (JavaTypeFactoryImpl.INSTANCE).
    return over;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable variable) {
    return new RexCorrelVariable(variable.id, variable.getType());
  }

}
