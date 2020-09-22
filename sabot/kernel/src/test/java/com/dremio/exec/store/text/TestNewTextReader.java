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
package com.dremio.exec.store.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.io.ByteOrderMark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopCompressionCodecFactory;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.easy.text.compliant.CompliantTextRecordReader;
import com.dremio.exec.store.easy.text.compliant.TextParsingSettings;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.test.AllocatorRule;
import com.dremio.test.UserExceptionMatcher;

public class TestNewTextReader extends BaseTestQuery {

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void fieldDelimiterWithinQuotes() throws Exception {
    testBuilder()
        .sqlQuery("select columns[1] as col1 from cp.\"textinput/input1.csv\"")
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("foo,bar")
        .go();
  }

  @Test
  public void testEmptyFileInFolder() throws Exception {
    File testFolder = tempDir.newFolder("testemptyfolder");
    File testEmptyPath1 = new File(testFolder, "testempty1.csv");
    testEmptyPath1.createNewFile();
    File testEmptyPath2 = new File(testFolder, "testempty2.csv");
    testEmptyPath2.createNewFile();
    PrintWriter pw = new PrintWriter(testEmptyPath2);
    // insert empty line
    pw.println();
    pw.println("VTS,2009-01-29 21:55:00");
    pw.println("VTS,2009-01-30 07:44:00");
    pw.close();

    testBuilder()
      .sqlQuery(String.format("select * from table(dfs.\"%s\" (type => 'text', fieldDelimiter => ',', " +
        "autoGenerateColumnNames => true))", testFolder.getAbsolutePath()))
      .unOrdered()
      .baselineColumns("A","B")
      .baselineValues(null, null)
      .baselineValues("VTS","2009-01-29 21:55:00")
      .baselineValues("VTS","2009-01-30 07:44:00")
      .go();
  }

  @Ignore ("Not needed any more. (DRILL-3178)")
  @Test
  public void ensureFailureOnNewLineDelimiterWithinQuotes() {
    try {
      test("select columns[1] as col1 from cp.\"textinput/input2.csv\"");
      fail("Expected exception not thrown.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot use newline character within quoted string"));
    }
  }

  @Test
  public void ensureColumnNameDisplayedinError() throws Exception {
    final String COL_NAME = "col1";

    try {
      test("select max(columns[1]) as %s from cp.\"textinput/input1.csv\" where %s is not null", COL_NAME, COL_NAME);
      fail("Query should have failed");
    } catch(UserRemoteException ex) {
      assertEquals(ErrorType.VALIDATION, ex.getErrorType());
      assertTrue("Error message should contain " + COL_NAME, ex.getMessage().contains(COL_NAME));
    }
  }

  @Test // see DRILL-3718
  public void testTabSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.tsv").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from dfs_test.\"%s\" ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test // see DRILL-3718
  public void testSpaceSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.ssv").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from TABLE(dfs_test.\"%s\"(type => 'TEXT', fieldDelimiter => ' ', lineDelimiter => '\n')) ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test // see DRILL-3718
  public void testPipSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuote.tbl").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
            "from dfs_test.\"%s\" ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .baselineValues("a", "a", "a")
        .build()
        .run();
  }

  @Test
  public void testValidateEmptyColumnNames() throws Exception {
    assertNull(null, CompliantTextRecordReader.validateColumnNames(null));
    assertEquals(0, CompliantTextRecordReader.validateColumnNames(new String[0]).length);
  }

  @Test
  public void testValidateColumnNamesSimple() throws Exception {
    String [] input = new String[] {"a", "b", "c"};
    String [] expected = new String[] {"a", "b", "c"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesDuplicate() throws Exception {
    String [] input = new String[] {"a", "b", "a", "b", "a", "a", "b", "c"};
    String [] expected = new String[] {"a", "b", "a0", "b0", "a1", "a2", "b1", "c"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesFillEmpty() throws Exception {
    String [] input = new String[] {"", "col1", "col2", "", "col3", ""};
    String [] expected = new String[] {"A", "col1", "col2", "D", "col3", "F"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test
  public void testValidateColumnNamesFillEmptyDuplicate() throws Exception {
    String [] input = new String[]    {"A", "", "", "B", "A", "B", "A", "",  "A", "B", "C", ""};
    String [] expected = new String[] {"A", "B", "C", "B0", "A0", "B1", "A1", "H", "A2", "B2", "C0", "L"};
    assertArrayEquals(expected, CompliantTextRecordReader.validateColumnNames(input));
  }

  @Test // see DRILL-3718
  public void testCrLfSeparatedWithQuote() throws Exception {
    final String root = FileUtils.getResourceAsFile("/store/text/WithQuotedCrLf.tbl").toURI().toString();
    final String query = String.format("select columns[0] as c0, columns[1] as c1, columns[2] as c2 \n" +
        "from dfs_test.\"%s\" ", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1", "c2")
        .baselineValues("a\n1", "a", "a")
        .baselineValues("a", "a\n2", "a")
        .baselineValues("a", "a", "a\n3")
        .build()
        .run();
  }

  @Test
  public void testBomUtf8() throws Exception {
    // Simple .csv file with a UTF-8 BOM. Should read successfully
    File testFolder = tempDir.newFolder("testUtf8Folder");
    File testFile = new File(testFolder, "utf8.csv");
    PrintStream p = new PrintStream(testFile);
    p.write(ByteOrderMark.UTF_8.getBytes(), 0, ByteOrderMark.UTF_8.length());
    p.print("A,B\n");
    p.print("5,7\n");
    p.close();

    testBuilder()
      .sqlQuery(String.format("select * from table(dfs.\"%s\" (type => 'text', " +
        "fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true))",
        testFile.getAbsolutePath()))
      .unOrdered()
      .baselineColumns("A","B")
      .baselineValues("5", "7")
      .go();
  }

  @Test
  public void testErrorBomUtf16() throws Exception {
    // UTF-16 BOM should cause a dataReadError user exception
    File testFolder = tempDir.newFolder("testUtf16Folder");
    File testFile = new File(testFolder, "utf16.csv");
    PrintStream p = new PrintStream(testFile);
    p.write(ByteOrderMark.UTF_16LE.getBytes(), 0, ByteOrderMark.UTF_16LE.length());
    p.print("A,B\n");
    p.print("5,7\n");
    p.close();

    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ,
      "DATA_READ ERROR: UTF-16 files not supported"));
    // NB: using test() instead of testBuilder() because it unwraps the thrown RpcException and re-throws the
    // underlying UserException (which is then matched with the UserExceptionMatcher)
    test(String.format("select * from table(dfs.\"%s\" (type => 'text', " +
        "fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true))",
      testFile.getAbsolutePath()));
  }

  @Test
  public void testShortFile() throws Exception {
    // short file: 2 characters worth (shorter than the UTF-8 BOM), without BOMs
    File testFolder = tempDir.newFolder("testShortFilesFolder");
    File testFile2 = new File(testFolder, "twobyte.csv");
    PrintStream p2 = new PrintStream(testFile2);
    p2.print("y\n");
    p2.close();

    testBuilder()
      .sqlQuery(String.format("select * from table(dfs.\"%s\" (type => 'text', " +
          "fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true)) ",
        testFile2.getAbsolutePath()))
      .unOrdered()
      .baselineColumns("y")
      .expectsEmptyResultSet()
      .go();
  }

  @Test
  public void testFileNotFound() {
    FileSplit split = mock(FileSplit.class);
    when(split.getPath()).thenReturn(new org.apache.hadoop.fs.Path("/notExist/notExitFile"));
    TextParsingSettings settings = mock(TextParsingSettings.class);
    when(settings.isHeaderExtractionEnabled()).thenReturn(true);
    SchemaPath column = mock(SchemaPath.class);
    List<SchemaPath> columns = new ArrayList<>(1);
    columns.add(column);
    SabotContext context = mock(SabotContext.class);
    try (BufferAllocator allocator = allocatorRule.newAllocator("test-new-text-reader", 0, Long.MAX_VALUE)) {
      when(context.getAllocator()).thenReturn(allocator);

      OptionManager optionManager = mock(OptionManager.class);
      when(optionManager.getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES))
        .thenReturn(ExecConstants.LIMIT_FIELD_SIZE_BYTES.getDefault().getNumVal());
      when(optionManager.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));

      Path path = Path.of("/notExist");
      try (BufferAllocator sampleAllocator = context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
           OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), sampleAllocator, optionManager, 1000);
           FileSystem dfs = HadoopFileSystem.get(path, new Configuration(), null);
           SampleMutator mutator = new SampleMutator(sampleAllocator);
           CompliantTextRecordReader reader = new CompliantTextRecordReader(split, HadoopCompressionCodecFactory.DEFAULT, dfs, operatorContext, settings, columns);
      ) {
        reader.setup(mutator);
      } catch (Exception e) {
        // java.io.FileNotFoundException is expected, but memory leak is not expected.
        assertTrue(e.getCause() instanceof FileNotFoundException);
      }
    }
  }

  @Test
  public void testRefreshOnFileNotFound() throws Exception {
    setEnableReAttempts(true);
    try {
      // create directory
      File testDir = tempDir.newFolder("testRefreshOnFileNotFound");

      // Create 2 text files in the directory.
      for (int i = 0; i < 2; ++i) {
        File testFile = new File(testDir, i + ".csv");
        PrintStream p = new PrintStream(testFile);

        p.write(ByteOrderMark.UTF_8.getBytes(), 0, ByteOrderMark.UTF_8.length());
        p.print("3,4\n");
        p.print("5,7\n");
        p.close();
      }

      // query on both files.
      testBuilder()
        .sqlQuery(String.format("select count(*) as c from dfs.\"%s\"", testDir.getAbsolutePath()))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(4L)
        .build()
        .run();

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // delete the second file.
      File testFile = new File(testDir, "1.csv");
      testFile.delete();

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // re-run the query. Should trigger a metadata refresh and succeed.
      testBuilder()
        .sqlQuery(String.format("select count(*) as c from dfs.\"%s\"", testDir.getAbsolutePath()))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();
    } finally {
      setEnableReAttempts(false);
    }
  }
}
