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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Tests for {@link DeltaMetadataFetchJobProducer}
 */

public class TestDeltaMetadataFetchJobProducer {

  @Test
  public void testReadingLatest() throws IOException {
    Path metaDir = Path.of("dummy");
    // Any dummy path will do for this test

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJobProducer producer = new DeltaMetadataFetchJobProducer(null, null, metaDir, 0L, true);

    assertTrue(producer.hasNext());
    DeltaMetadataFetchJob job = producer.next();

    assertEquals(producer.currentVersion(), 1L);

    assertTrue(producer.hasNext());
    assertTrue(producer.hasNext());

    assertEquals(producer.currentVersion(), 1L);

    producer.next();

    assertEquals(producer.currentVersion(), 2L);
  }

  @Test
  public void testReadingSnapShot() throws IOException {
    Path metaDir = Path.of("dummy");
    // Any dummy path will do for this test

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJobProducer producer = new DeltaMetadataFetchJobProducer(null, null, metaDir, 4L, false);

    assertTrue(producer.hasNext());
    DeltaMetadataFetchJob job = producer.next();

    assertEquals(producer.currentVersion(), 3L);


    while (producer.hasNext()) {
      producer.next();
    }

    assertEquals(producer.currentVersion(), -1L);
    assertEquals(producer.hasNext(), false);
  }

  @Test
  public void testProducedJobConfig() throws IOException {
    Path metaDir = Path.of("dummy");
    // Any dummy path will do for this test
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJobProducer producer1 = new DeltaMetadataFetchJobProducer(null, null, metaDir, 4L, true);

    DeltaMetadataFetchJob job = producer1.next();
    //Moving forward then first job should be a checkpoint read
    assertEquals(job.tryCheckpointRead, true);

    //Now other jobs should be commit reads
    job = producer1.next();
    assertEquals(job.tryCheckpointRead, false);

    job = producer1.next();
    assertEquals(job.tryCheckpointRead, false);


    DeltaMetadataFetchJobProducer producer2 = new DeltaMetadataFetchJobProducer(null, null, metaDir, 0L, true);

    job = producer2.next();
    //starting from 0 then even the first job should be commit read
    assertEquals(job.tryCheckpointRead, false);

    job = producer2.next();
    assertEquals(job.tryCheckpointRead, false);

    DeltaMetadataFetchJobProducer producer3 = new DeltaMetadataFetchJobProducer(null, null, metaDir, 9L, false);
    //Moving backwards then we will try and read both
    while(producer3.hasNext()) {
      job = producer3.next();
      assertEquals(job.tryCheckpointRead, true);
    }
  }
}

