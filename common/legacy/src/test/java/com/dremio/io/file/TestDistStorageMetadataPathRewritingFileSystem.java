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
package com.dremio.io.file;


import java.io.IOException;
import java.net.URI;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the path rewrite functions in PathRewriteFileSystem class.
 * Specifically, The two methods tested are: reWritePathIfNecessary, findMetadataOccurrences.
 */
public class TestDistStorageMetadataPathRewritingFileSystem {


  /**
   * tests hypothetical where original metadata and distributed storage metadata locations are different.
   * test should ensure the path is rewritten. Substitute the originalPath prefix with the distributed storage location
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath1() throws IOException {
    Path distStoragePath = Path.of("/my.server.com/user/Dog/metadata");
    Path originalPath    = Path.of("/my.server.com/user/Blah/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");
    Path expect          = Path.of("/my.server.com/user/Dog/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests hypothetical where original metadata and distributed storage metadata locations are different.
   * testPathRewriteFeature2 is similar to testPathRewriteFeature1, but variable currentMetaPath has an additional
   * "metadata/example/metadata/" in the path to handle complex edge case to identify the proper start to the guid.
   * test should ensure the path is rewritten. Substitute the currentMetaPath prefix with the distributed storage location
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath2() throws IOException {
    Path distStoragePath  = Path.of("/my.server.com/user/Dog/metadata");
    Path originalPath     = Path.of("/my.server.com/user/Blah/metadata/example/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");
    Path expect           = Path.of("/my.server.com/user/Dog/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests hypothetical where original metadata and distributed storage metadata locations are different.
   * testPathRewriteFeature3 is similar to testPathRewriteFeature1, but variable distributedStorage has an additional
   * "metadata/example/metadata/" in the path to handle complex edge case to identify the proper start to the guid.
   * test should ensure the path is rewritten. Substitute the currentMetaPath prefix with the distributed storage location
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath3() throws IOException {
    Path distStoragePath  = Path.of("/my.server.com/user/Dog/metadata/example/metadata");
    Path originalPath     = Path.of("/my.server.com/user/Blah/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");
    Path expect           = Path.of("/my.server.com/user/Dog/metadata/example/metadata/" +
      "f30f893e-ed43-491a-9a3d-34698f55a922/metadata/00000-d92a0e7b-6995-4dba-8452-58e11b6465f8.metadata.json");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests hypothetical where original metadata is null. This enacts a possible case of dremio starting up for the first time
   * test should return the currentMetaPath: null
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath4() throws IOException {
    Path distStoragePath  = Path.of("/fake.server.com/user/Dog/metadata");
    Path originalPath     = null;
    Path expect           = null;

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests the hypothetical where distributed storage is null. This enacts a possible case of dremio starting up for the first time.
   * test should not conduct a path rewrite.
   * @throws IOException when path does not match an existing file location
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRewrittenPath5() throws IOException {
    Path distStoragePath    = null;
    Path originalPath       = Path.of("/fake.server.com/user/Blah/metadata/");
    Path expect             = originalPath;

    FileSystem mockFs = Mockito.mock(FileSystem.class);
    DistStorageMetadataPathRewritingFileSystem distStorageMetadataPathRewritingFileSystem = new DistStorageMetadataPathRewritingFileSystem(mockFs, distStoragePath);
  }

  /**
   * tests the hypothetical where the original metadata differs from the distributed storage, but the guid is not present
   * test should not conduct a path rewrite.
   * Hypothetical may not exist - It was not seen/found through debugging process. Added for robustness.
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath6() throws IOException {
    Path distStoragePath    = Path.of("/fake.server.com/user/Dog/metadata");
    Path originalPath       = Path.of("/fake.server.com/user/Blah/metadata");
    Path expect             = originalPath;

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * Test Hypothetical where user specifies a unique distributed storage path which contains
   * METADATA in the original iceberg path name.
   * expected to return early with an unchanged path
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath7() throws IOException {
    Path distStoragePath = Path.of("/Users/usr/myNAS/NEW/metadata/guid/metadata/metadata/my.json");
    Path originalPath    = Path.of("/Users/usr/myNAS/J/metadata/guid/metadata/METADATA/my.json");
    Path expect          = Path.of("/Users/usr/myNAS/J/metadata/guid/metadata/METADATA/my.json");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * Test Hypothetical where original metadata and distributed storage are identical.
   * expected to return early with an unchanged path
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath8() throws IOException {
    Path distStoragePath  = Path.of("/A/metadata");
    Path originalPath     = Path.of("/A/metadata");
    Path expect           = Path.of("/A/metadata");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests the hypothetical where the original metadata differs from the distributed storage, but the guid is not present.
   * test should not conduct a path rewrite.
   * Hypothetical may not exist - It was not seen/found through debugging process. Added for robustness.
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath9() throws IOException {
    Path distStoragePath  = Path.of("/A/metadata");
    Path originalPath     = Path.of("/A/metadata/a0967a24-9942-4c64-913f-970e396a1026");
    Path expect           = originalPath;

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests hypothetical of an HDFS distributed storage config.
   * Path is expected to rewrite to the new distStoragePath root AND maintain the "hdfs://localhost:8020" connection Prefix
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath10() throws IOException {
    Path distStoragePath  = Path.of("/B/metadata");
    URI distStoreURI      = Path.of("hdfs://localhost:8020/B/metadata").toURI();
    Path originalPath     = Path.of("hdfs://localhost:8020/A/metadata/e2e18049-c899-4d26-9cce-b5c554b00941/metadata/8396ac6a-86ac-4382-a9b1-f881e9ae6fb4.avro");
    Path expect           = Path.of("hdfs://localhost:8020/B/metadata/e2e18049-c899-4d26-9cce-b5c554b00941/metadata/8396ac6a-86ac-4382-a9b1-f881e9ae6fb4.avro");


    test(distStoragePath, originalPath, expect, distStoreURI);
  }

  /**
   * Tests hypothetical where user alters the dist-storage metadata directory to {dist.path}/"subdir"
   * test should not conduct a path rewrite and return early.
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath11() throws IOException {
    Path distStoragePath  = Path.of("/B/metadata/example/metadata/subdir");
    Path originalPath     = Path.of("/B/metadata/example/metadata/subdir");
    Path expect           = Path.of("/B/metadata/example/metadata/subdir");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * Tests hypothetical NAS distributed Storage Configuration
   * Test expected to re-write path with new distStoragePath prefix
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath12() throws IOException {
    Path distStoragePath  = Path.of("/Users/usr/myNAS/M/metadata");
    Path originalPath     = Path.of("/Users/usr/myNAS/J/metadata/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");
    Path expect           = Path.of("/Users/usr/myNAS/M/metadata/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * Tests hypothetical where user alters the dist-storage metadata directory to {dist.path}/"my.path"
   * while the original Path root dir differs.
   * Handles complex case where user's custom distributed storage metadata path contains a period, ".",
   * at the leaf. This test challenges the integrity of the rewriteFileSystemIfNecessary by ensuring path is still properly rewritten
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath13() throws IOException {
    Path distStoragePath  = Path.of("/Users/usr/myNAS/M/metadata/my.path");
    Path originalPath     = Path.of("/Users/usr/myNAS/J/metadata/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");
    Path expect           = Path.of("/Users/usr/myNAS/M/metadata/my.path/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * Tests hypothetical where user alters the dist-storage metadata directory to {dist.path}/"my.path"
   * while the original Path root dir matches.
   * Handles complex case where user's custom distributed storage metadata path contains a period, ".",
   * at the leaf. This test challenges the integrity of the rewriteFileSystemIfNecessary by ensuring path is NOT rewritten
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath14() throws IOException {
    Path distStoragePath = Path.of("/Users/usr/myNAS/J/metadata/my.path");
    Path originalPath    = Path.of("/Users/usr/myNAS/J/metadata/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");
    Path expect          = Path.of("/Users/usr/myNAS/J/metadata/my.path/75d8ae04-e260-480a-ab18-e7f96fe052f7/metadata/01ece669-0de7-4ac5-bc5d-20a419c256d7.avro");

    test(distStoragePath, originalPath, expect, distStoragePath.toURI());
  }

  /**
   * tests hypothetical of an HDFS distributed storage config.
   * Path is expected to rewrite to the new distStoragePath WHICH INCLUDES the new hdfs authentication schema
   * @throws IOException when path does not match an existing file location
   */
  @Test
  public void testRewrittenPath15() throws IOException {
    Path distStoragePath  = Path.of("/B/metadata");
    URI distStoreURI      = Path.of("hdfs://fakenode-0.c.noexist-1234.internal:5555/B/metadata").toURI();
    Path originalPath     = Path.of("hdfs://localhost:8020/A/metadata/e2e18049-c899-4d26-9cce-b5c554b00941/metadata/8396ac6a-86ac-4382-a9b1-f881e9ae6fb4.avro");
    Path expect           = Path.of("hdfs://fakenode-0.c.noexist-1234.internal:5555/B/metadata/e2e18049-c899-4d26-9cce-b5c554b00941/metadata/8396ac6a-86ac-4382-a9b1-f881e9ae6fb4.avro");

    test(distStoragePath, originalPath, expect, distStoreURI);
  }

  /**
   * Conducts the path rewrite algorithm for testing. Given the previous metadata path and the new storage,
   * we generate a mock instance of our new FileSystem implementation, then invoke one of the File System's methods.
   * The test passes if the path variable is rewritten to the expected path
   * @param distStoragePath : the current/new  distributed storage root path
   * @param originalPath : the table's original iceberg metadata path
   * @param expect : the expected path once the test exits the path-rewrite algorithm
   * @throws IOException
   */
  private void test(Path distStoragePath, Path originalPath, Path expect, URI uri) throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.when(mockFs.getUri()).thenReturn(uri);
    DistStorageMetadataPathRewritingFileSystem distStorageMetadataPathRewritingFileSystem = new DistStorageMetadataPathRewritingFileSystem(mockFs, distStoragePath);
    FileAttributes actual = distStorageMetadataPathRewritingFileSystem.getFileAttributes(originalPath);
    Mockito.verify(mockFs).getFileAttributes(expect);
  }
}
