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
package com.dremio.dac.server;

import org.hibernate.validator.cfg.ConstraintDef;

/**
 * Constraint definition for TransformGroupByConstraint.
 */
public class TransformGroupByConstraintDef extends ConstraintDef<TransformGroupByConstraintDef, TransformGroupByConstraint> {

  public TransformGroupByConstraintDef() {
    super(TransformGroupByConstraint.class);
  }

  @Override
  protected TransformGroupByConstraintDef addParameter(String key, Object value) {
    return super.addParameter(key, value);
  }
}
