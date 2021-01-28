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

import com.dremio.common.util.FileUtils;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Tests for {@link DeltaLogSnapshot}
 */
public class TestDeltaLogSnapshot {

    @Test
    public void testMerge() throws IOException {
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot0 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test2_init.json").toURI()));
        DeltaLogSnapshot snapshot1 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test1_3.json").toURI()));
        DeltaLogSnapshot snapshot2 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test1_4.json").toURI()));

        DeltaLogSnapshot snapshot00 = snapshot0.clone();
        snapshot00.merge(snapshot1);
        snapshot00.merge(snapshot2);

        assertEquals("COMBINED", snapshot00.getOperationType());
        assertEquals((snapshot0.getNetFilesAdded() + snapshot1.getNetFilesAdded() + snapshot2.getNetFilesAdded()), snapshot00.getNetFilesAdded());
        assertEquals(snapshot0.getNetOutputRows() + snapshot1.getNetOutputRows() + snapshot2.getNetOutputRows(), snapshot00.getNetOutputRows());
        assertEquals(snapshot0.getNetBytesAdded() + snapshot1.getNetBytesAdded() + snapshot2.getNetBytesAdded(), snapshot00.getNetBytesAdded());
        assertEquals(snapshot2.getSchema(), snapshot00.getSchema());
        assertEquals(snapshot2.getTimestamp(), snapshot00.getTimestamp());
        assertEquals(snapshot2.getVersionId(), snapshot00.getVersionId());
        assertEquals(snapshot2.getPartitionColumns(), snapshot00.getPartitionColumns());

        // Different permutations of the merge
        DeltaLogSnapshot snapshot10 = snapshot1.clone();
        DeltaLogSnapshot snapshot20 = snapshot2.clone();
        snapshot20.merge(snapshot0);
        snapshot10.merge(snapshot20);
        assertEquals(snapshot00, snapshot10);
    }

    @Test
    public void testCompare() throws IOException {
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot0 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test2_init.json").toURI()));
        DeltaLogSnapshot snapshot1 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test1_3.json").toURI()));
        DeltaLogSnapshot snapshot2 = jsonReader.parseMetadata(fs, Path.of(FileUtils.getResourceAsFile("/deltalake/test1_4.json").toURI()));

        assertTrue(snapshot0.compareTo(snapshot2) < 0);
        assertTrue(snapshot1.compareTo(snapshot2) < 0);
        assertTrue(snapshot1.compareTo(snapshot0) > 0);
        assertTrue(snapshot0.clone().compareTo(snapshot0) == 0);
    }
}
