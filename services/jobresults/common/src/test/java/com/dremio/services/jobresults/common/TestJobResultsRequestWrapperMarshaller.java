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
package com.dremio.services.jobresults.common;

import com.dremio.common.utils.protos.QueryWritableBatch;
import java.io.InputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test class for {@link JobResultsRequestWrapperMarshaller} */
public class TestJobResultsRequestWrapperMarshaller {
  private static BufferAllocator allocator;
  private static JobResultsRequestWrapperMarshaller marshaller;
  private static final int COUNT = 5;

  @BeforeClass
  public static void setup() throws Exception {
    allocator = new RootAllocator();
    marshaller = new JobResultsRequestWrapperMarshaller(allocator);
  }

  @AfterClass
  public static void teardown() throws Exception {
    marshaller.close();
    allocator.close();
  }

  private JobResultsRequestWrapper createRequest() {
    QueryWritableBatch data = JobResultsTestUtils.createQueryWritableBatch(allocator, COUNT);
    long sequenceId = 100;
    return new JobResultsRequestWrapper(
        data.getHeader(), sequenceId, data.getBuffers(), JobResultsTestUtils.getSampleForeman());
  }

  @Test
  public void testMarshaller() throws Exception {
    JobResultsRequestWrapper wrapper = createRequest();
    InputStream stream = marshaller.stream(wrapper);

    JobResultsRequestWrapper wrapperFromStream = marshaller.parse(stream);

    Assert.assertTrue(
        "Stream() and parse() of JobResultsRequestWrapperMarshaller seems not consistent.",
        wrapper.equals(wrapperFromStream));

    wrapperFromStream.close();
    stream.close();
  }
}
