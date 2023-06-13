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
package com.dremio.service.reflection.refresh;


import org.junit.Assert;
import org.junit.Test;

import com.dremio.service.reflection.proto.Materialization;

public class TestRefreshDoneHandler {
  @Test
  public void testGetIsEmptyReflection() {

    final Materialization materialization = new Materialization();
    //test with new materialization
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, true,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(true, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(true, true,materialization));

    //test with null isIcebergDataset
    materialization.setIsIcebergDataset(null);
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, true,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(true, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(true, true,materialization));

    //test with Iceberg serialization
    materialization.setIsIcebergDataset(true);
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(false, true,materialization));
    //this is the only case that is true, it is false in all other cases
    Assert.assertTrue(RefreshDoneHandler.getIsEmptyReflection(true, false,materialization));
    Assert.assertFalse(RefreshDoneHandler.getIsEmptyReflection(true, true,materialization));
  }
}
