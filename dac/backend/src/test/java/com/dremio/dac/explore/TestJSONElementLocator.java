/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.explore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.explore.JSONElementLocator.ArrayJsonPathElement;
import com.dremio.dac.explore.JSONElementLocator.Interval;
import com.dremio.dac.explore.JSONElementLocator.JsonPath;
import com.dremio.dac.explore.JSONElementLocator.JsonSelection;
import com.dremio.dac.explore.JSONElementLocator.ObjectJsonPathElement;
import com.dremio.dac.explore.model.extract.Selection;
import com.fasterxml.jackson.core.JsonParseException;

/**
 * Tests the JSONElementLocator
 */
public class TestJSONElementLocator {

  public void validateSelection(Selection selection, String expected) throws JsonParseException, IOException {
    JSONElementLocator locator = new JSONElementLocator(selection.getCellText());
    JsonSelection location = locator.locate(selection.getOffset(), selection.getOffset() + selection.getLength());
    String selectedText = selection.getCellText().substring(selection.getOffset(), selection.getOffset() + selection.getLength());
    assertEquals(expected, selectedText + " " + location);
  }

  @Test
  public void testListSelection() throws Exception {
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 10, 3), "bar Selection[s:[1], e:[1]]");
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 3, 3), "foo Selection[s:[0], e:[0]]");
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 17, 3), "baz Selection[s:[2], e:[2]]");
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 10, 10), "bar\", \"baz Selection[s:[1], e:[2]]");
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 9, 12), "\"bar\", \"baz\" Selection[s:[1], e:[2]]");
    validateSelection(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 11, 8), "ar\", \"ba Selection[s:[1], e:[2]]");
    validateSelection(new Selection("foo", "{ \"bar\" :[ \"foo\", \"bar\", \"baz\" ]}", 18, 12), "\"bar\", \"baz\" Selection[s:.bar[1], e:.bar[2]]");
  }

  @Test
  public void testMapSelection() throws Exception {
    validateSelection(new Selection("a", "{\"Tuesday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}", 11, 32), "{\"close\":\"19:00\",\"open\":\"10:00\"} Selection[s:.Tuesday, e:.Tuesday]");
  }

  @Test
  public void testMapHighlightInt() throws Exception {
    validateSelection(".Price Range", "1", "{\"Ambience\":{\"romantic\":false,\"intimate\":false,\"touristy\":false,\"hipster\":false,\"divey\":false,\"classy\":false,\"trendy\":false,\"upscale\":false,\"casual\":false},\"Coat Check\":false,\"Price Range\":1,\"Music\":{},\"Good For\":{},\"Parking\":{},\"Hair Types Specialized In\":{}}");
    validateSelection(".Price Range", "1", "{\"Ambience\":{\"romantic\":false,\"intimate\":false,\"touristy\":false,\"hipster\":false,\"divey\":false,\"classy\":false,\"trendy\":false,\"upscale\":false,\"casual\":false},\"Coat Check\":false,\"Price Range\": 1   ,\"Music\":{},\"Good For\":{},\"Parking\":{},\"Hair Types Specialized In\":{}}");
  }
  @Test
  public void testMapHighlightMap() throws Exception {
    validateSelection(".Tuesday", "{\"close\":\"19:00\",\"open\":\"10:00\"}", "{\"Tuesday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
    validateSelection(".Monday", "{}", "{\"Tuesday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
  }
  @Test
  public void testMapHighlightString() throws Exception {
    validateSelection(".Tuesday.close", "19:00", "{\"Tuesday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
    validateSelection(".Tuesday.close", "19:00", "{\"Tuesday\":{\"close\":    \"19:00\"     ,\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
  }
  @Test
  public void testMapHighlightBool() throws Exception {
    validateSelection(".Tuesday.bool", "true", "{\"Tuesday\":{\"bool\":true,\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
    validateSelection(".Tuesday.bool", "false", "{\"Tuesday\":{\"bool\":    false     ,\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
  }
  @Test
  public void testMapHighlightArray() throws Exception {
    validateSelection(".Tuesday.array", "[\"a\"]", "{\"Tuesday\":{\"array\":[\"a\"],\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
    validateSelection(".Tuesday.array", "[ \"a\", \"b\"  ]", "{\"Tuesday\":{\"array\":  [ \"a\", \"b\"  ]   ,\"open\":\"10:00\"},\"Friday\":{\"close\":\"19:00\",\"open\":\"10:00\"},\"Monday\":{}}");
  }
  @Test
  public void testMapHighlightFloat() throws Exception {
    validateSelection(".Price Range", "1.1", "{\"Ambience\":{\"romantic\":false,\"intimate\":false,\"touristy\":false,\"hipster\":false,\"divey\":false,\"classy\":false,\"trendy\":false,\"upscale\":false,\"casual\":false},\"Coat Check\":false,\"Price Range\":1.1,\"Music\":{},\"Good For\":{},\"Parking\":{},\"Hair Types Specialized In\":{}}");
    validateSelection(".Price Range", "1.2", "{\"Ambience\":{\"romantic\":false,\"intimate\":false,\"touristy\":false,\"hipster\":false,\"divey\":false,\"classy\":false,\"trendy\":false,\"upscale\":false,\"casual\":false},\"Coat Check\":false,\"Price Range\": 1.2   ,\"Music\":{},\"Good For\":{},\"Parking\":{},\"Hair Types Specialized In\":{}}");
  }
  @Test
  public void testMapHighlightList() throws Exception {
    validateSelection(".a", "[]", "{\"a\":[]}");
    validateSelection(".a", "[\"romantic\", false]", "{\"a\":[\"romantic\", false]}");
  }
  @Test
  public void testListHighlightString() throws Exception {
    validateSelection("[0]", "a", "[\"a\", \"b\"]");
    validateSelection("[0]", "a", "[\"a\"]");
  }
  @Test
  public void testListHighlightInt() throws Exception {
    validateSelection("[0]", "0", "[0, \"b\"]");
    validateSelection("[2]", "1", "[\"a\", true,   1   ]");
  }
  @Test
  public void testListHighlightFloat() throws Exception {
    validateSelection("[0]", "0.1", "[0.1, \"b\"]");
  }
  @Test
  public void testListHighlightBool() throws Exception {
    validateSelection("[1]", "true", "[0.1, true ]");
    validateSelection("[2]", "false", "[0.1, true, false ]");
  }
  @Test
  public void testListHighlightList() throws Exception {
    validateSelection("[1]", "[]", "[0.1, [] ]");
    validateSelection("[2]", "[ false ]", "[0.1, true, [ false ] ]");
  }
  private void validateSelection(String path, String textValue, String text) throws Exception {
    JSONElementLocator locator = new JSONElementLocator(text);
    JsonPath searchedPath = new JsonPath(path);
    Interval location = locator.locatePath(searchedPath);
    Assert.assertNotNull(location);
    String selectedText = text.substring(location.getStart(), location.getEnd());
    assertEquals(textValue, selectedText);
  }

  @Test
  public void testParseJsonPath() throws Exception {
    JsonPath p = JSONElementLocator.parsePath("value.a");
    assertEquals(p.toString(), 1, p.size());
    assertEquals(p.toString(), "a", p.last().asObject().getField());
  }

  @Test
  public void testParseJsonPath2() throws Exception {
    JsonPath p = JSONElementLocator.parsePath("value.a.b.c");
    assertEquals(p.toString(), 3, p.size());
    assertEquals(new JsonPath(new ObjectJsonPathElement("a"), new ObjectJsonPathElement("b"), new ObjectJsonPathElement("c")), p);
  }

  @Test
  public void testParseJsonPath3() throws Exception {
    JsonPath p = JSONElementLocator.parsePath("value[0][1][2]");
    assertEquals(p.toString(), 3, p.size());
    assertEquals(new JsonPath(new ArrayJsonPathElement(0), new ArrayJsonPathElement(1), new ArrayJsonPathElement(2)), p);
  }

  @Test
  public void testParseJsonPath4() throws Exception {
    JsonPath p = JSONElementLocator.parsePath("value[0].a[1]");
    assertEquals(p.toString(), 3, p.size());
    assertEquals(new JsonPath(new ArrayJsonPathElement(0), new ObjectJsonPathElement("a"), new ArrayJsonPathElement(1)), p);
  }

  @Test
  public void testParseJsonPath5() throws Exception {
    JsonPath p = JSONElementLocator.parsePath("value.a[0].b");
    assertEquals(p.toString(), 3, p.size());
    assertEquals(new JsonPath(new ObjectJsonPathElement("a"), new ArrayJsonPathElement(0), new ObjectJsonPathElement("b")), p);
  }
}
