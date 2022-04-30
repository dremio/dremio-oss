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
package com.dremio.sabot.exec.cursors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.sabot.threads.sharedres.SharedResourceType;

public class TestFileCursorManager {
  private final FileStreamManager streamManager = mock(FileStreamManager.class);
  private final SharedResourceManager resourceManager = SharedResourceManager.newBuilder()
    .addGroup("test")
    .build();

  @Test
  public void testWriter() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer observer = mgr.registerWriter(streamManager,
      createSharedResource("testWriter"), () -> {})) {

      mgr.notifyAllRegistrationsDone();
      observer.updateCursor(0, 10);
      assertEquals(10, mgr.getWriteCursor());

      observer.updateCursor(0, 20);
      assertEquals(20, mgr.getWriteCursor());

      assertEquals(-1, mgr.getMaxReadCursor());
      assertEquals(streamManager, mgr.getFileStreamManager());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWriterDupRegister() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer observer = mgr.registerWriter(streamManager,
      createSharedResource("testWriterDupRegister"), () -> {})) {

      FileCursorManager.Observer observer2 = mgr.registerWriter(streamManager,
        createSharedResource("testWriterDupRegister2"), () -> {
        });
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriterBadCursor() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer observer = mgr.registerWriter(streamManager,
      createSharedResource("testWriterBadCursor"), () -> {})) {

      observer.updateCursor(0, 10);
      // cursor cannot reduce in value.
      observer.updateCursor(0, 9);
    }
  }

  @Test
  public void testReader() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer reader = mgr.registerReader(createSharedResource("testReader:reader"));
         FileCursorManager.Observer writer = mgr.registerWriter(streamManager, createSharedResource("testReader:writer"), () -> {})) {

      mgr.notifyAllRegistrationsDone();
      writer.updateCursor(0, 10);
      assertEquals(10, mgr.getWriteCursor());

      reader.updateCursor(0, 5);
      assertEquals(5, mgr.getMaxReadCursor());

      reader.updateCursor(0, 7);
      assertEquals(7, mgr.getMaxReadCursor());
      assertEquals(10, mgr.getWriteCursor());

      reader.updateCursor(0, 10);
      assertEquals(10, mgr.getMaxReadCursor());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReaderBadCursor() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer reader = mgr.registerReader(createSharedResource("reader"));
         FileCursorManager.Observer writer = mgr.registerWriter(streamManager, createSharedResource("writer"), () -> {
         })) {

      mgr.notifyAllRegistrationsDone();
      writer.updateCursor(0, 10);
      assertEquals(10, mgr.getWriteCursor());

      // reader cannot go ahead of the writer.
      reader.updateCursor(0, 12);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReaderBadCursor2() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    try (FileCursorManager.Observer reader = mgr.registerReader(createSharedResource("reader"));
         FileCursorManager.Observer writer = mgr.registerWriter(streamManager, createSharedResource("writer"), () -> {
         })) {

      mgr.notifyAllRegistrationsDone();
      writer.updateCursor(0, 10);
      assertEquals(10, mgr.getWriteCursor());

      // reader cursor cannot decrease.
      reader.updateCursor(0, 7);
      reader.updateCursor(0, 6);
    }
  }

  @Test
  public void testSingleReaderFlowControl() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    SharedResource writeResource = createSharedResource("writer");
    SharedResource readResource = createSharedResource("reader");

    try (FileCursorManager.Observer reader = mgr.registerReader(readResource);
         FileCursorManager.Observer writer = mgr.registerWriter(streamManager, writeResource, () -> {})) {

      mgr.notifyAllRegistrationsDone();
      // initially, reader is blocked.
      assertTrue(writeResource.isAvailable());
      assertFalse(readResource.isAvailable());

      // reader should be unblocked after writer moves.
      writer.updateCursor(0, 2);
      assertEquals(-1, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertTrue(readResource.isAvailable());

      // reader should be blocked if it catches up with the writer.
      reader.updateCursor(0, 2);
      assertEquals(2, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertFalse(readResource.isAvailable());

      // writer should be blocked if it moves too much ahead of the reader.
      writer.updateCursor(0, 10);
      assertEquals(2, mgr.getMaxReadCursor());
      assertFalse(writeResource.isAvailable());
      assertTrue(readResource.isAvailable());

      // writer should be unblocked if the reader comes close.
      reader.updateCursor(0, 9);
      assertEquals(9, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertTrue(readResource.isAvailable());

      // reader should be blocked if it catches up with the writer.
      reader.updateCursor(0, 10);
      assertEquals(10, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertFalse(readResource.isAvailable());
    }
  }

  @Test
  public void testMultiReaderFlowControl() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    SharedResource writeResource = createSharedResource("writer");
    SharedResource readResource1 = createSharedResource("reader1");
    SharedResource readResource2 = createSharedResource("reader2");

    try (FileCursorManager.Observer reader1 = mgr.registerReader(readResource1);
         FileCursorManager.Observer reader2 = mgr.registerReader(readResource2);
         FileCursorManager.Observer writer = mgr.registerWriter(streamManager, writeResource, () -> {})) {

      mgr.notifyAllRegistrationsDone();
      // initially, readers are blocked.
      assertEquals(-1, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertFalse(readResource1.isAvailable());
      assertFalse(readResource2.isAvailable());

      // readers should be unblocked after writer moves.
      writer.updateCursor(0, 2);
      assertTrue(writeResource.isAvailable());
      assertTrue(readResource1.isAvailable());
      assertTrue(readResource2.isAvailable());

      // reader should be blocked if it catches up with the writer.
      reader1.updateCursor(0, 2);
      assertEquals(2, mgr.getMaxReadCursor());
      assertFalse(readResource1.isAvailable());
      assertTrue(readResource2.isAvailable());

      reader2.updateCursor(0, 2);
      assertEquals(2, mgr.getMaxReadCursor());
      assertFalse(readResource1.isAvailable());
      assertFalse(readResource2.isAvailable());
      assertTrue(writeResource.isAvailable());

      // writer should be blocked if it moves too much ahead of the readers.
      writer.updateCursor(0, 10);
      assertFalse(writeResource.isAvailable());

      // writer should be unblocked if one of the readers comes close.
      reader2.updateCursor(0, 9);
      assertEquals(9, mgr.getMaxReadCursor());
      assertTrue(writeResource.isAvailable());
      assertTrue(readResource1.isAvailable());
      assertTrue(readResource2.isAvailable());
    }
  }

  @Test
  public void testAllReadersDoneCallback() throws Exception {
    FileCursorManager mgr = new FileCursorManagerImpl("test");
    SharedResource writeResource = createSharedResource("writer");
    SharedResource readResource1 = createSharedResource("reader1");
    SharedResource readResource2 = createSharedResource("reader2");
    AtomicBoolean allReadersDone = new AtomicBoolean(false);

    try (FileCursorManager.Observer writer = mgr.registerWriter(streamManager, writeResource, () -> {
      allReadersDone.set(true);
    })) {
      try (FileCursorManager.Observer reader1 = mgr.registerReader(readResource1)) {
        try (FileCursorManager.Observer reader2 = mgr.registerReader(readResource2)) {
          mgr.notifyAllRegistrationsDone();

          writer.updateCursor(0, 10);
          assertFalse(allReadersDone.get());

          reader1.updateCursor(0, 10);
          reader2.updateCursor(0, 10);
          assertFalse(allReadersDone.get());
        }
        assertFalse(allReadersDone.get());
      }

      // both readers have closed, the writer must have got the callback.
      assertTrue(allReadersDone.get());
    }
  }

  @Test
  public void testStreamManagerDeleteAll() throws Exception {
    AtomicBoolean deleteAllDone = new AtomicBoolean(false);
    final FileStreamManager streamManagerImpl = new FileStreamManager() {
      @Override
      public String getId() {
        return null;
      }

      @Override
      public OutputStream createOutputStream(int fileSeq) throws IOException {
        return null;
      }

      @Override
      public InputStream getInputStream(int fileSeq) throws IOException {
        return null;
      }

      @Override
      public void delete(int fileSeq) throws IOException {
      }

      @Override
      public void deleteAll() throws IOException {
        deleteAllDone.set(true);
      }
    };

    FileCursorManager mgr = new FileCursorManagerImpl("test");
    SharedResource writeResource = createSharedResource("writer");
    SharedResource readResource1 = createSharedResource("reader1");
    SharedResource readResource2 = createSharedResource("reader2");

    try (FileCursorManager.Observer writer = mgr.registerWriter(streamManagerImpl, writeResource, () -> {})) {
      try (FileCursorManager.Observer reader1 = mgr.registerReader(readResource1)) {
        try (FileCursorManager.Observer reader2 = mgr.registerReader(readResource2)) {
          mgr.notifyAllRegistrationsDone();

          writer.updateCursor(0, 10);
          assertFalse(deleteAllDone.get());

          reader1.updateCursor(0, 10);
          reader2.updateCursor(0, 10);
          assertFalse(deleteAllDone.get());
        }
        assertFalse(deleteAllDone.get());
      }
      assertFalse(deleteAllDone.get());
    }
    // the writer and both the readers have closed, so the deleteAll callback must have occurred.
    assertTrue(deleteAllDone.get());
  }

  SharedResource createSharedResource(String name) {
    return resourceManager.getGroup("test").createResource(name, SharedResourceType.TEST);
  }

}
