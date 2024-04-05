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
package com.dremio.service.nessie.upgrade.version040;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.service.nessie.upgrade.version040.model.DeleteOperation;
import com.dremio.service.nessie.upgrade.version040.model.NessieCommit;
import com.dremio.service.nessie.upgrade.version040.model.PutOperation;
import com.dremio.service.nessie.upgrade.version040.model.UnchangedOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import org.junit.jupiter.api.Test;

public class TestObjectMapping {
  private static final String CURRENT_HASH =
      "5146f0179215b054fa0133689e04b8f72b517f711b897977050f7e910409f8fa";
  private static final String PREVIOUS_HASH =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";
  private static final String METADATA_HASH = "5a19cce5-48d4-415b-9fb0-b3b3efa8938d";
  private static final String COMMITER = "Dremio User";
  private static final String EMAIL = "foo@bar.com";
  private static final String MESSAGE = "foo";
  private static final long COMMIT_TIME = 1644357675949L;
  private static final String KEY_PART1 = "foo";
  private static final String KEY_PART2 = "bar";
  private static final String LOCATION = "/foo/bar/baz";

  private static final String COMMIT1 =
      "{\n"
          + "        \"hash\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + CURRENT_HASH
          + "\",\n"
          + "                \"hash\":\""
          + CURRENT_HASH
          + "\"\n"
          + "        },\n"
          + "        \"ancestor\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + PREVIOUS_HASH
          + "\",\n"
          + "                \"hash\":\""
          + PREVIOUS_HASH
          + "\"\n"
          + "        },\n"
          + "        \"metadata\":{\n"
          + "                \"hash\":\""
          + METADATA_HASH
          + "\",\n"
          + "                \"commiter\":\""
          + COMMITER
          + "\",\n"
          + "                \"email\":\""
          + EMAIL
          + "\",\n"
          + "                \"message\":\""
          + MESSAGE
          + "\",\n"
          + "                \"commitTime\":"
          + COMMIT_TIME
          + "\n"
          + "        },\n"
          + "        \"operations\":{\n"
          + "                \"operations\":[\n"
          + "                        {\n"
          + "                                \"type\":\"PUT\",\n"
          + "                                \"key\":{\n"
          + "                                        \"elements\":[\n"
          + "                                                \""
          + KEY_PART1
          + "\",\""
          + KEY_PART2
          + "\"\n"
          + "                                        ]\n"
          + "                                },\n"
          + "                                \"contents\":{\n"
          + "                                        \"type\":\"ICEBERG_TABLE\",\n"
          + "                                        \"metadataLocation\":\""
          + LOCATION
          + "\"\n"
          + "                                }\n"
          + "                        }\n"
          + "                ]\n"
          + "        }\n"
          + "}";
  private static final String COMMIT2 =
      "{\n"
          + "        \"hash\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + CURRENT_HASH
          + "\",\n"
          + "                \"hash\":\""
          + CURRENT_HASH
          + "\"\n"
          + "        },\n"
          + "        \"metadata\":{\n"
          + "                \"hash\":\""
          + METADATA_HASH
          + "\",\n"
          + "                \"commiter\":\""
          + COMMITER
          + "\",\n"
          + "                \"email\":\""
          + EMAIL
          + "\",\n"
          + "                \"message\":\""
          + MESSAGE
          + "\",\n"
          + "                \"commitTime\":"
          + COMMIT_TIME
          + "\n"
          + "        },\n"
          + "        \"operations\":{\n"
          + "                \"operations\":[\n"
          + "                        {\n"
          + "                                \"type\":\"PUT\",\n"
          + "                                \"key\":{\n"
          + "                                        \"elements\":[\n"
          + "                                                \""
          + KEY_PART1
          + "\",\""
          + KEY_PART2
          + "\"\n"
          + "                                        ]\n"
          + "                                },\n"
          + "                                \"contents\":{\n"
          + "                                        \"type\":\"ICEBERG_TABLE\",\n"
          + "                                        \"metadataLocation\":\""
          + LOCATION
          + "\"\n"
          + "                                }\n"
          + "                        }\n"
          + "                ]\n"
          + "        }\n"
          + "}";
  private static final String COMMIT3 =
      "{\n"
          + "        \"hash\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + CURRENT_HASH
          + "\",\n"
          + "                \"hash\":\""
          + CURRENT_HASH
          + "\"\n"
          + "        },\n"
          + "        \"ancestor\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + PREVIOUS_HASH
          + "\",\n"
          + "                \"hash\":\""
          + PREVIOUS_HASH
          + "\"\n"
          + "        },\n"
          + "        \"metadata\":{\n"
          + "                \"hash\":\""
          + METADATA_HASH
          + "\",\n"
          + "                \"commiter\":\""
          + COMMITER
          + "\",\n"
          + "                \"email\":\""
          + EMAIL
          + "\",\n"
          + "                \"message\":\""
          + MESSAGE
          + "\",\n"
          + "                \"commitTime\":"
          + COMMIT_TIME
          + "\n"
          + "        },\n"
          + "        \"operations\":{\n"
          + "                \"operations\":[\n"
          + "                        {\n"
          + "                                \"type\":\"DELETE\",\n"
          + "                                \"key\":{\n"
          + "                                        \"elements\":[\n"
          + "                                                \""
          + KEY_PART1
          + "\",\""
          + KEY_PART2
          + "\"\n"
          + "                                        ]\n"
          + "                                }\n"
          + "                        }\n"
          + "                ]\n"
          + "        }\n"
          + "}";
  private static final String COMMIT4 =
      "{\n"
          + "        \"hash\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + CURRENT_HASH
          + "\",\n"
          + "                \"hash\":\""
          + CURRENT_HASH
          + "\"\n"
          + "        },\n"
          + "        \"ancestor\":{\n"
          + "                \"type\":\"HASH\",\n"
          + "                \"name\":\""
          + PREVIOUS_HASH
          + "\",\n"
          + "                \"hash\":\""
          + PREVIOUS_HASH
          + "\"\n"
          + "        },\n"
          + "        \"metadata\":{\n"
          + "                \"hash\":\""
          + METADATA_HASH
          + "\",\n"
          + "                \"commiter\":\""
          + COMMITER
          + "\",\n"
          + "                \"email\":\""
          + EMAIL
          + "\",\n"
          + "                \"message\":\""
          + MESSAGE
          + "\",\n"
          + "                \"commitTime\":"
          + COMMIT_TIME
          + "\n"
          + "        },\n"
          + "        \"operations\":{\n"
          + "                \"operations\":[\n"
          + "                        {\n"
          + "                                \"type\":\"UNCHANGED\",\n"
          + "                                \"key\":{\n"
          + "                                        \"elements\":[\n"
          + "                                                \""
          + KEY_PART1
          + "\",\""
          + KEY_PART2
          + "\"\n"
          + "                                        ]\n"
          + "                                }\n"
          + "                        }\n"
          + "                ]\n"
          + "        }\n"
          + "}";

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testPutWithAncestor() throws Exception {
    final NessieCommit commit =
        objectMapper.readValue(new StringReader(COMMIT1), NessieCommit.class);

    assertEquals(CURRENT_HASH, commit.getHash().getName());
    assertEquals(CURRENT_HASH, commit.getHash().getHash());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getName());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getHash());
    assertEquals(METADATA_HASH, commit.getMetadata().getHash());
    assertEquals(COMMITER, commit.getMetadata().getCommiter());
    assertEquals(EMAIL, commit.getMetadata().getEmail());
    assertEquals(MESSAGE, commit.getMetadata().getMessage());
    assertEquals(COMMIT_TIME, commit.getMetadata().getCommitTime());
    assertEquals(1, commit.getOperations().getOperations().size());
    assertTrue(commit.getOperations().getOperations().get(0) instanceof PutOperation);

    final PutOperation putOperation = (PutOperation) commit.getOperations().getOperations().get(0);
    assertEquals(2, putOperation.getKey().getElements().size());
    assertEquals(KEY_PART1, putOperation.getKey().getElements().get(0));
    assertEquals(KEY_PART2, putOperation.getKey().getElements().get(1));
    assertEquals(LOCATION, putOperation.getContents().getMetadataLocation());
  }

  @Test
  public void testPutWithoutAncestor() throws Exception {
    final NessieCommit commit =
        objectMapper.readValue(new StringReader(COMMIT2), NessieCommit.class);

    assertEquals(CURRENT_HASH, commit.getHash().getName());
    assertEquals(CURRENT_HASH, commit.getHash().getHash());
    assertNull(commit.getAncestor());
    assertEquals(METADATA_HASH, commit.getMetadata().getHash());
    assertEquals(COMMITER, commit.getMetadata().getCommiter());
    assertEquals(EMAIL, commit.getMetadata().getEmail());
    assertEquals(MESSAGE, commit.getMetadata().getMessage());
    assertEquals(COMMIT_TIME, commit.getMetadata().getCommitTime());
    assertEquals(1, commit.getOperations().getOperations().size());
    assertTrue(commit.getOperations().getOperations().get(0) instanceof PutOperation);

    final PutOperation putOperation = (PutOperation) commit.getOperations().getOperations().get(0);
    assertEquals(2, putOperation.getKey().getElements().size());
    assertEquals(KEY_PART1, putOperation.getKey().getElements().get(0));
    assertEquals(KEY_PART2, putOperation.getKey().getElements().get(1));
    assertEquals(LOCATION, putOperation.getContents().getMetadataLocation());
  }

  @Test
  public void testDeleteWithAncestor() throws Exception {
    final NessieCommit commit =
        objectMapper.readValue(new StringReader(COMMIT3), NessieCommit.class);

    assertEquals(CURRENT_HASH, commit.getHash().getName());
    assertEquals(CURRENT_HASH, commit.getHash().getHash());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getName());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getHash());
    assertEquals(METADATA_HASH, commit.getMetadata().getHash());
    assertEquals(COMMITER, commit.getMetadata().getCommiter());
    assertEquals(EMAIL, commit.getMetadata().getEmail());
    assertEquals(MESSAGE, commit.getMetadata().getMessage());
    assertEquals(COMMIT_TIME, commit.getMetadata().getCommitTime());
    assertEquals(1, commit.getOperations().getOperations().size());
    assertTrue(commit.getOperations().getOperations().get(0) instanceof DeleteOperation);
    assertEquals(2, commit.getOperations().getOperations().get(0).getKey().getElements().size());
    assertEquals(
        KEY_PART1, commit.getOperations().getOperations().get(0).getKey().getElements().get(0));
    assertEquals(
        KEY_PART2, commit.getOperations().getOperations().get(0).getKey().getElements().get(1));
  }

  @Test
  public void testUnchangedWithAncestor() throws Exception {
    final NessieCommit commit =
        objectMapper.readValue(new StringReader(COMMIT4), NessieCommit.class);

    assertEquals(CURRENT_HASH, commit.getHash().getName());
    assertEquals(CURRENT_HASH, commit.getHash().getHash());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getName());
    assertEquals(PREVIOUS_HASH, commit.getAncestor().getHash());
    assertEquals(METADATA_HASH, commit.getMetadata().getHash());
    assertEquals(COMMITER, commit.getMetadata().getCommiter());
    assertEquals(EMAIL, commit.getMetadata().getEmail());
    assertEquals(MESSAGE, commit.getMetadata().getMessage());
    assertEquals(COMMIT_TIME, commit.getMetadata().getCommitTime());
    assertEquals(1, commit.getOperations().getOperations().size());
    assertTrue(commit.getOperations().getOperations().get(0) instanceof UnchangedOperation);
    assertEquals(2, commit.getOperations().getOperations().get(0).getKey().getElements().size());
    assertEquals(
        KEY_PART1, commit.getOperations().getOperations().get(0).getKey().getElements().get(0));
    assertEquals(
        KEY_PART2, commit.getOperations().getOperations().get(0).getKey().getElements().get(1));
  }
}
