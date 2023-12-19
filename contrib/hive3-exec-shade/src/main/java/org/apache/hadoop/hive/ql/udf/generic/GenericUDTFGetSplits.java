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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
/**
 * Dummy for GenericUDTFGetSplits as the original depends on LLAP libraries that are excluded in hive3-exec-shade.
 */
@Description(name = "get_splits", value = "_FUNC_(string,int) - "
    + "Dummy UDTF.")
@UDFType(deterministic = false)
public class GenericUDTFGetSplits extends GenericUDTF {

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    return null;
  }

  @Override
  public void process(Object[] arguments) throws HiveException {
  }

  @Override
  public void close() throws HiveException {
  }
}
