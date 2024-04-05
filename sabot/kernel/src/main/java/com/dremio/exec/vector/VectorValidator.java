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
package com.dremio.exec.vector;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import org.apache.arrow.vector.ValueVector;

public class VectorValidator {
  public static void validate(VectorAccessible batch) {
    int count = batch.getRecordCount();
    long hash = 12345;
    SelectionVectorMode mode = batch.getSchema().getSelectionVectorMode();
    switch (mode) {
      case NONE:
        {
          for (VectorWrapper w : batch) {
            ValueVector v = w.getValueVector();
            for (int i = 0; i < count; i++) {
              Object obj = v.getObject(i);
              if (obj != null) {
                hash = obj.hashCode() ^ hash;
              }
            }
          }
          break;
        }
      case TWO_BYTE:
        {
          for (VectorWrapper w : batch) {
            ValueVector v = w.getValueVector();
            for (int i = 0; i < count; i++) {
              int index = batch.getSelectionVector2().getIndex(i);
              Object obj = v.getObject(index);
              if (obj != null) {
                hash = obj.hashCode() ^ hash;
              }
            }
          }
          break;
        }
      case FOUR_BYTE:
        {
          for (VectorWrapper w : batch) {
            ValueVector[] vv = w.getValueVectors();
            for (int i = 0; i < count; i++) {
              int index = batch.getSelectionVector4().get(i);
              ValueVector v = vv[index >> 16];
              Object obj = v.getObject(index & 65535);
              if (obj != null) {
                hash = obj.hashCode() ^ hash;
              }
            }
          }
        }
    }
    if (hash == 0) {
      //      System.out.println(hash);
    }
  }
}
