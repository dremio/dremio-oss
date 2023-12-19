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
package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.impl.array.ArrayHelper;

public class ArrayConcatUtility {
  public static void concat(
    FieldReader arrayReader1,
    FieldReader arrayReader2,
    BaseWriter.ComplexWriter writer,
    FunctionErrorContext errorContext) {
    UnionListReader listReader1 = (UnionListReader) arrayReader1;
    UnionListReader listReader2 = (UnionListReader) arrayReader2;

    BaseWriter.ListWriter listWriter = writer.rootAsList();
    listWriter.startList();

    while (listReader1.next()){
      ArrayHelper.copyListElements(listReader1, listWriter);
    }

    while (listReader2.next()){
      ArrayHelper.copyListElements(listReader2, listWriter);
    }

    listWriter.endList();
  }
}
