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
package com.dremio.exec.fn.hive;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.util.List;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

public class TestHiveUDFs extends BaseTestQuery {

  @Test
  public void testGenericUDF() throws Throwable {
    int numRecords = 0;
    String planString = readResourceAsString("functions/hive/GenericUDF.json");
    List<QueryDataBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getTestAllocator());
    for (QueryDataBatch result : results) {
      batchLoader.load(result.getHeader().getDef(), result.getData());
      if (batchLoader.getRecordCount() <= 0) {
        result.release();
        batchLoader.clear();
        continue;
      }
      // Output columns and types
      //  1. str1 : VarChar
      //  2. upperStr1 : VarChar
      //  3. concat : VarChar
      //  4. flt1 : Float4
      //  5. format_number : Float8
      //  6. nullableStr1 : VarChar
      //  7. upperStr1 : VarChar
      VarCharVector str1V =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 0).getValueVector();
      VarCharVector upperStr1V =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 1).getValueVector();
      VarCharVector concatV =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 2).getValueVector();
      Float4Vector flt1V =
          (Float4Vector) batchLoader.getValueAccessorById(Float4Vector.class, 3).getValueVector();
      VarCharVector formatNumberV =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 4).getValueVector();
      VarCharVector nullableStr1V =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 5).getValueVector();
      VarCharVector upperNullableStr1V =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 6).getValueVector();

      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        if (str1V.isNull(i)) {
          continue;
        }
        String in = new String(str1V.get(i), UTF_8);
        String upper = new String(upperStr1V.get(i), UTF_8);
        assertEquals(in.toUpperCase(), upper);

        String concat = new String(concatV.get(i), UTF_8);
        assertEquals(in + "-" + in, concat);

        float flt1 = flt1V.get(i);
        String formatNumber = new String(formatNumberV.get(i), UTF_8);

        String nullableStr1 = null;
        if (!nullableStr1V.isNull(i)) {
          nullableStr1 = new String(nullableStr1V.get(i), UTF_8);
        }

        String upperNullableStr1 = null;
        if (!upperNullableStr1V.isNull(i)) {
          upperNullableStr1 = new String(upperNullableStr1V.get(i), UTF_8);
        }

        assertEquals(nullableStr1 != null, upperNullableStr1 != null);
        if (nullableStr1 != null) {
          assertEquals(nullableStr1.toUpperCase(), upperNullableStr1);
        }

        numRecords++;
      }

      result.release();
      batchLoader.clear();
    }
  }

  @Test
  public void testUDF() throws Throwable {
    int numRecords = 0;
    String planString = readResourceAsString("functions/hive/UDF.json");
    List<QueryDataBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getTestAllocator());
    for (QueryDataBatch result : results) {
      batchLoader.load(result.getHeader().getDef(), result.getData());
      if (batchLoader.getRecordCount() <= 0) {
        result.release();
        batchLoader.clear();
        continue;
      }

      // Output columns and types
      // 1. str1 : VarChar
      // 2. str1Length : Int
      // 3. str1Ascii : Int
      // 4. flt1 : Float4
      // 5. pow : Float8
      VarCharVector str1V =
          (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 0).getValueVector();
      IntVector str1LengthV =
          (IntVector) batchLoader.getValueAccessorById(IntVector.class, 1).getValueVector();
      IntVector str1AsciiV =
          (IntVector) batchLoader.getValueAccessorById(IntVector.class, 2).getValueVector();
      Float4Vector flt1V =
          (Float4Vector) batchLoader.getValueAccessorById(Float4Vector.class, 3).getValueVector();
      Float8Vector powV =
          (Float8Vector) batchLoader.getValueAccessorById(Float8Vector.class, 4).getValueVector();

      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        if (str1V.isNull(i)) {
          continue;
        }
        String str1 = new String(str1V.get(i), UTF_8);
        long str1Length = str1LengthV.get(i);
        assertTrue(str1.length() == str1Length);

        int str1Ascii = str1AsciiV.get(i);

        float flt1 = flt1V.get(i);

        double pow = 0;
        if (!powV.isNull(i)) {
          pow = powV.get(i);
          assertTrue(Math.pow(flt1, 2.0) == pow);
        }
        numRecords++;
      }

      result.release();
      batchLoader.clear();
    }
  }
}
