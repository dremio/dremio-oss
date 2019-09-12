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
package com.dremio.exec.util;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.User;
import org.apache.parquet.hadoop.CodecFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.VectorContainer;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.google.common.collect.Lists;


/**
 * Tests for global dictionary utilities.
 */
public class TestGlobalDictionaryBuilder extends DremioTest {

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  private static Path tableDirPath;
  private static Path partitionDirPath;
  private static Configuration conf;
  private static FileSystem fs;
  private static final Charset UTF8 = Charset.forName("UTF-8");

  private static String [] kinds = { "landline", "mobile", "cell", "work", "home"};

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    fs = HadoopFileSystem.getLocal(conf);
    File tableDir = folder.newFolder("t1");
    File partitionDir = folder.newFolder("p1");

    List<User> users1 = Lists.newArrayList();

    // user names repeat in first file should create a dictionary
    for (int i = 0; i < 100; i++) {
      PhoneBookWriter.Location location = new PhoneBookWriter.Location((double)i % 5, (double) i% 6);
      users1.add(new PhoneBookWriter.User(i, "p" + i%10, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, kinds[i%2])), location));
    }

    List<User> users2  = Lists.newArrayList();
    for (int i = 100; i < 500; i++) {
      PhoneBookWriter.Location location = new PhoneBookWriter.Location((double)i, (double) i);
      users2.add(new PhoneBookWriter.User(i, "p" + i, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, kinds[i%3])), location));
    }

    List<User> users3  = Lists.newArrayList();
    for (int i = 100; i < 500; i++) {
      PhoneBookWriter.Location location = new PhoneBookWriter.Location((double)i, (double) i);
      users3.add(new PhoneBookWriter.User(i, "p" + i, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, kinds[i%4])), location));
    }

    // new partition which uses a last value from kind
    List<User> users4  = Lists.newArrayList();
    for (int i = 500; i < 600; i++) {
      PhoneBookWriter.Location location = new PhoneBookWriter.Location((double)i, (double) i);
      users4.add(new PhoneBookWriter.User(i, "p" + i, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, kinds[i%5])), location));
    }

    File f1 = new File(tableDir.getAbsoluteFile(), "phonebook1.parquet");
    File f2 = new File(tableDir.getAbsoluteFile(), "phonebook2.parquet");
    File f3 = new File(tableDir.getAbsoluteFile(), "phonebook3.parquet");
    File f4 = new File(partitionDir.getAbsoluteFile(), "phonebook4.parquet");

    PhoneBookWriter.writeToFileWithPageHeaders(users1, 100, 1024*1024).renameTo(f1);
    PhoneBookWriter.writeToFileWithPageHeaders(users2, 100, 1024*1024).renameTo(f2);
    PhoneBookWriter.writeToFileWithPageHeaders(users3, 100, 1024*1024).renameTo(f3);
    PhoneBookWriter.writeToFileWithPageHeaders(users4, 100, 1024*1024).renameTo(f4);

    tableDirPath = Path.of(tableDir.getAbsolutePath());
    partitionDirPath = Path.of(partitionDir.getAbsolutePath());
  }

  @AfterClass
  public static void cleanup() throws IOException {
    fs.delete(tableDirPath, true);
    fs.delete(partitionDirPath, true);
  }

  @Test
  public void testDictionaryRootDirFiltering() throws Exception {
    Path tabledir = Path.of(folder.newFolder("testDictionaryRootDirFiltering").getAbsolutePath());
    // initial no dictionaries present
    assertEquals(-1, GlobalDictionaryBuilder.getDictionaryVersion(fs, tabledir));

    assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 0,
      Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
    assertEquals(0, GlobalDictionaryBuilder.getDictionaryVersion(fs, tabledir));

    assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 1,
      Path.of(folder.newFolder("testDictionaryRootDirFiltering_0").getAbsolutePath())));
    assertEquals(1, GlobalDictionaryBuilder.getDictionaryVersion(fs, tabledir));

    try {
      assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 0,
        Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
      fail("dictionaries for version 0 already exist, operation should fail");
    } catch (IOException ioe) {
    }

    try {
      assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 1,
        Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
      fail("dictionaries for version 0 already exist, operation should fail");
    } catch (IOException ioe) {
    }

    assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 10,
      Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
    assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 12,
      Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
    try {
      assertNotNull(GlobalDictionaryBuilder.createDictionaryVersionedRootPath(fs, tabledir, 9,
        Path.of(folder.newFolder(UUID.randomUUID().toString()).getAbsolutePath())));
      fail("dictionaries with a higher version 12 already exist, operation should fail");
    } catch (IOException ioe) {
    }
    assertEquals(12, GlobalDictionaryBuilder.getDictionaryVersion(fs, tabledir));
  }

  @Test
  public void testDictionaryFileName() throws Exception {
    assertEquals("_foo.dict", GlobalDictionaryBuilder.dictionaryFileName("foo"));
    assertEquals("_a.b.c.dict", GlobalDictionaryBuilder.dictionaryFileName("a.b.c"));
    assertEquals("_foo.dict", GlobalDictionaryBuilder.dictionaryFileName(new ColumnDescriptor(new String[]{"foo"}, INT64, 0, 1)));
    assertEquals("_a.b.c.dict", GlobalDictionaryBuilder.dictionaryFileName(new ColumnDescriptor(new String[]{"a", "b", "c"}, INT64, 0, 1)));
  }

  @Test
  public void testExtractColumnName() throws Exception {
    assertEquals("foo", GlobalDictionaryBuilder.getColumnFullPath("_foo.dict"));
    assertEquals("a.b.c", GlobalDictionaryBuilder.getColumnFullPath("_a.b.c.dict"));
  }

  @Test
  public void testGlobalDictionary() throws Exception {
    try (final BufferAllocator bufferAllocator = allocatorRule.newAllocator("test-global-dictionary-builder", 0, Long.MAX_VALUE)) {
      final CompressionCodecFactory codec = CodecFactory.createDirectCodecFactory(conf, new ParquetDirectByteBufferAllocator(bufferAllocator), 0);
      Map<ColumnDescriptor, Path> globalDictionaries = GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, tableDirPath, bufferAllocator).getColumnsToDictionaryFiles();
      assertEquals(1, globalDictionaries.size());
      ColumnDescriptor column = globalDictionaries.entrySet().iterator().next().getKey();
      assertTrue(Arrays.equals(new String[] {"phoneNumbers", "phone", "kind"}, column.getPath()));
      long dictionaryVersion = GlobalDictionaryBuilder.getDictionaryVersion(fs, tableDirPath);
      assertEquals(0, dictionaryVersion);
      Path dictionaryRootPath = GlobalDictionaryBuilder.getDictionaryVersionedRootPath(fs, tableDirPath, dictionaryVersion);
      try (final VectorContainer dict = GlobalDictionaryBuilder.readDictionary(fs, dictionaryRootPath, column, bufferAllocator)) {
        assertEquals(4, dict.getRecordCount());
        final VarBinaryVector dictValues = dict.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
        assertEquals("cell", new String(dictValues.get(0), UTF8));
        assertEquals("landline", new String(dictValues.get(1), UTF8));
        assertEquals("mobile", new String(dictValues.get(2), UTF8));
        assertEquals("work", new String(dictValues.get(3), UTF8));
      }
      assertEquals(1, GlobalDictionaryBuilder.listDictionaryFiles(fs, dictionaryRootPath).size());

      // update global dictionary
      globalDictionaries = GlobalDictionaryBuilder.updateGlobalDictionaries(codec, fs, tableDirPath, partitionDirPath, bufferAllocator).getColumnsToDictionaryFiles();
      assertEquals(1, globalDictionaries.size());
      column = globalDictionaries.entrySet().iterator().next().getKey();
      assertTrue(Arrays.equals(new String[] {"phoneNumbers", "phone", "kind"}, column.getPath()));
      dictionaryVersion = GlobalDictionaryBuilder.getDictionaryVersion(fs, tableDirPath);
      dictionaryRootPath = GlobalDictionaryBuilder.getDictionaryVersionedRootPath(fs, tableDirPath, dictionaryVersion);
      assertEquals(1, dictionaryVersion);
      try (final VectorContainer dict = GlobalDictionaryBuilder.readDictionary(fs, dictionaryRootPath, column, bufferAllocator)) {
        assertEquals(5, dict.getRecordCount());
        final VarBinaryVector dictValues = dict.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
        assertEquals("cell", new String(dictValues.get(0), UTF8));
        assertEquals("home", new String(dictValues.get(1), UTF8));
        assertEquals("landline", new String(dictValues.get(2), UTF8));
        assertEquals("mobile", new String(dictValues.get(3), UTF8));
        assertEquals("work", new String(dictValues.get(4), UTF8));
      }

      assertEquals(1, GlobalDictionaryBuilder.listDictionaryFiles(fs, dictionaryRootPath).size());
      assertEquals(0, GlobalDictionaryBuilder.listDictionaryFiles(fs, partitionDirPath).size());
    }
  }

  @Test
  public void testLocalDictionaries() throws IOException {
    try (final BufferAllocator bufferAllocator = allocatorRule.newAllocator("test-global-dictionary-builder", 0, Long.MAX_VALUE)) {
      final CompressionCodecFactory codecFactory = CodecFactory.createDirectCodecFactory(conf, new ParquetDirectByteBufferAllocator(bufferAllocator), 0);
      Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> dictionaries1 =
        LocalDictionariesReader.readDictionaries(fs, tableDirPath.resolve("phonebook1.parquet"), codecFactory);
      Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> dictionaries2 =
        LocalDictionariesReader.readDictionaries(fs, tableDirPath.resolve("phonebook2.parquet"), codecFactory);
      Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> dictionaries3 =
        LocalDictionariesReader.readDictionaries(fs, tableDirPath.resolve("phonebook3.parquet"), codecFactory);
      Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> dictionaries4 =
        LocalDictionariesReader.readDictionaries(fs, partitionDirPath.resolve("phonebook4.parquet"), codecFactory);

      assertEquals(2, dictionaries1.getKey().size()); // name and kind have dictionaries
      assertEquals(1, dictionaries2.getKey().size());
      assertEquals(1, dictionaries3.getKey().size());
      assertEquals(1, dictionaries4.getKey().size());

      assertEquals(0, dictionaries1.getValue().size());
      assertEquals(1, dictionaries2.getValue().size()); // skip name
      assertEquals(1, dictionaries3.getValue().size()); // skip name
      assertEquals(1, dictionaries4.getValue().size()); // skip name
    }
  }
}
