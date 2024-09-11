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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.service.jobs.JobStatusListener;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * External reflection tests on versioned datasets. Currently, external reflections on versioned
 * datasets is not supported so just verify that DDL query fails. In the future, when we do support
 * this feature, add tests here.
 */
public class ITTestExternalReflections extends ITBaseTestReflection {
  private BufferAllocator allocator;
  private static final String tableName = generateUniqueTableName();
  private static final List<String> tablePath =
      tablePathWithSource(DATAPLANE_PLUGIN_NAME, Collections.singletonList(tableName));

  @BeforeEach
  public void setUp() {
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    allocator.close();
  }

  /** Verifies DDL for external reflection on Versioned Dataset errors out */
  @Test
  public void testExternalReflectionFails() {
    assertThatThrownBy(
            () ->
                JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
                    getJobsService(),
                    createNewJobRequestFromSql(
                        createExternalReflection(tablePath, tablePath, "ext_ref")),
                    JobStatusListener.NO_OP))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "External reflections are not supported on versioned source dataPlane_Test");
  }

  public static String createExternalReflection(
      final List<String> query, final List<String> target, final String name) {
    return String.format(
        "ALTER DATASET %s CREATE EXTERNAL REFLECTION %s USING %s",
        joinedTableKey(query), name, joinedTableKey(target));
  }
}
