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
package com.dremio.exec.store.copyinto;

import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PrimitiveTypeReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Stopwatch;
import java.util.List;

public class CopyIntoTransformationPrimitiveTypeReader extends PrimitiveTypeReader {

  public CopyIntoTransformationPrimitiveTypeReader(
      SampleMutator mutator,
      OperatorContext context,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      BatchSchema originalSchema,
      List<CopyIntoTransformationProperties.Property> transformationProperties,
      BatchSchema targetSchema,
      int depth) {
    super(
        mutator,
        context,
        typeCoercion,
        javaCodeGenWatch,
        gandivaCodeGenWatch,
        originalSchema,
        depth);
    initReaders(
        mutator,
        context,
        typeCoercion,
        javaCodeGenWatch,
        gandivaCodeGenWatch,
        targetSchema,
        transformationProperties,
        depth);
  }

  protected void initReaders(
      SampleMutator mutator,
      OperatorContext context,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      BatchSchema targetSchema,
      List<CopyIntoTransformationProperties.Property> transformationProperties,
      int depth) {
    nonVarcharCoercionReader =
        new CopyIntoTransformationNonVarcharCoercionReader(
            mutator,
            context,
            nonVarcharSchema,
            typeCoercion,
            javaCodeGenWatch,
            gandivaCodeGenWatch,
            transformationProperties,
            targetSchema,
            depth);
  }
}
