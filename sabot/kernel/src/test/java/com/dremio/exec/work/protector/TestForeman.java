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

package com.dremio.exec.work.protector;

import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Test class for unit testing helper functions in {@link Foreman}
 */
public class TestForeman {

  @Test
  public void testDatasetValidityChecker() {
    UserException invalidMetadataException = UserException.invalidMetadataError()
                                                          .message("Invalid metadata exception without rawAdditionalContext.")
                                                          .buildSilently();
    Predicate<DatasetConfig> datasetValidityChecker = Foreman.getDatasetValidityChecker(invalidMetadataException,
                                                                                        AttemptReason.INVALID_DATASET_METADATA);
    assertEquals("Since rawAdditionalContext is null in invalidMetadataError, returned predicate should be true",
                 Predicates.alwaysTrue(),
                 datasetValidityChecker);
  }

}
