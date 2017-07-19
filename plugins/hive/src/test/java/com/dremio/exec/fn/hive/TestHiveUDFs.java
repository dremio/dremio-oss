/*
 * Copyright (C) 2017 Dremio Corporation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class TestHiveUDFs extends BaseTestQuery {

  @Test
  public void testGenericUDF() throws Throwable {

    int numRecords = 0;
    String planString = Resources.toString(Resources.getResource("functions/hive/GenericUDF.json"), Charsets.UTF_8);
    List<QueryDataBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    for (QueryDataBatch result : results) {
      batchLoader.load(result.getHeader().getDef(), result.getData());
      if (batchLoader.getRecordCount() <= 0) {
        result.release();
        batchLoader.clear();
        continue;
      }
      // Output columns and types
      //  1. str1 : VarChar
      //  2. upperStr1 : NullableVarChar
      //  3. concat : NullableVarChar
      //  4. flt1 : Float4
      //  5. format_number : NullableFloat8
      //  6. nullableStr1 : NullableVarChar
      //  7. upperNullableStr1 : NullableVarChar
      NullableVarCharVector str1V = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 0).getValueVector();
      NullableVarCharVector upperStr1V = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 1).getValueVector();
      NullableVarCharVector concatV = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 2).getValueVector();
      NullableFloat4Vector flt1V = (NullableFloat4Vector) batchLoader.getValueAccessorById(NullableFloat4Vector.class, 3).getValueVector();
      NullableVarCharVector format_numberV = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 4).getValueVector();
      NullableVarCharVector nullableStr1V = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 5).getValueVector();
      NullableVarCharVector upperNullableStr1V = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 6).getValueVector();

      for (int i=0; i<batchLoader.getRecordCount(); i++) {
        if (str1V.getAccessor().isNull(i)) {
          continue;
        }
        String in = new String(str1V.getAccessor().get(i), Charsets.UTF_8);
        String upper = new String(upperStr1V.getAccessor().get(i), Charsets.UTF_8);
        assertTrue(in.toUpperCase().equals(upper));


        String concat = new String(concatV.getAccessor().get(i), Charsets.UTF_8);
        assertTrue(concat.equals(in+"-"+in));

        float flt1 = flt1V.getAccessor().get(i);
        String format_number = new String(format_numberV.getAccessor().get(i), Charsets.UTF_8);


        String nullableStr1 = null;
        if (!nullableStr1V.getAccessor().isNull(i)) {
          nullableStr1 = new String(nullableStr1V.getAccessor().get(i), Charsets.UTF_8);
        }

        String upperNullableStr1 = null;
        if (!upperNullableStr1V.getAccessor().isNull(i)) {
          upperNullableStr1 = new String(upperNullableStr1V.getAccessor().get(i), Charsets.UTF_8);
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
    String planString = Resources.toString(Resources.getResource("functions/hive/UDF.json"), Charsets.UTF_8);
    List<QueryDataBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
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
      NullableVarCharVector str1V = (NullableVarCharVector) batchLoader.getValueAccessorById(NullableVarCharVector.class, 0).getValueVector();
      NullableIntVector str1LengthV = (NullableIntVector) batchLoader.getValueAccessorById(NullableIntVector.class, 1).getValueVector();
      NullableIntVector str1AsciiV = (NullableIntVector) batchLoader.getValueAccessorById(NullableIntVector.class, 2).getValueVector();
      NullableFloat4Vector flt1V = (NullableFloat4Vector) batchLoader.getValueAccessorById(NullableFloat4Vector.class, 3).getValueVector();
      NullableFloat8Vector powV = (NullableFloat8Vector) batchLoader.getValueAccessorById(NullableFloat8Vector.class, 4).getValueVector();

      for (int i=0; i<batchLoader.getRecordCount(); i++) {
        if (str1V.getAccessor().isNull(i)) {
          continue;
        }
        String str1 = new String(str1V.getAccessor().get(i), Charsets.UTF_8);
        long str1Length = str1LengthV.getAccessor().get(i);
        assertTrue(str1.length() == str1Length);

        int str1Ascii = str1AsciiV.getAccessor().get(i);

        float flt1 = flt1V.getAccessor().get(i);

        double pow = 0;
        if (!powV.getAccessor().isNull(i)) {
          pow = powV.getAccessor().get(i);
          assertTrue(Math.pow(flt1, 2.0) == pow);
        }
        numRecords++;
      }

      result.release();
      batchLoader.clear();
    }
  }

}
