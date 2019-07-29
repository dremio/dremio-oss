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
package com.dremio.dac.explore;

import static com.dremio.dac.explore.Transformer.VALUE_PLACEHOLDER;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

/**
 * Locate the json elements selected in text.
 */
public class JSONElementLocator {

  private final String cellText;

  /** statefull parser */
  private JsonParser parser;
  /** current path in the json document */
  private JsonPath path;
  /** name of the current field if we are in an object */
  private String currentField = null;
  /** true if we are just passed a START_ARRAY token
   * at the first value in the array the path gets appended [0] */
  private boolean startedArray = false;

  /**
   * @param text the text to inspect
   */
  public JSONElementLocator(String text) {
    this.cellText = text;
  }

  public static JsonPath parsePath(String path) {
    if (path.startsWith(VALUE_PLACEHOLDER)) {
      return new JsonPath(path.substring(VALUE_PLACEHOLDER.length()));
    }
    throw new IllegalArgumentException(path + " must start with 'value'");
  }

  private JSONElementLocator.ArrayJsonPathElement popArray() {
    if (startedArray) {
      startedArray = false;
      return new ArrayJsonPathElement();
    } else if (path.path.peek().isArray()) {
      return path.path.pop().asArray();
    } else {
      throw new IllegalStateException(String.format("%s was not in an array: %s", path, cellText));
    }
  }

  private JSONElementLocator.ObjectJsonPathElement popObject() {
    if (!startedArray && path.path.peek().isObject()) {
      return path.path.pop().asObject();
    } else {
      throw new IllegalStateException("was not in an object: " + path );
    }
  }

  private void pushObject(String field) {
    path.path.push(new ObjectJsonPathElement(field));
  }

  private void pushArray() {
    startedArray = true;
    // we want to have the path unchanged until the first value
  }

  /**
   * if we are in an array a new value will increment its index
   */
  private void inc() {
    if (startedArray) {
      // if we had a new array starting we add an array element to the path
      path.path.push(new ArrayJsonPathElement());
      startedArray = false;
    }
    if (path.size() == 0) {
      // this was just the root of the object
      return;
    }
    if (path.last().isArray()) {
      path.path.push(popArray().inc());
    }
  }

  /**
   * find the interval in the cellText passed to the constructor that delimit the element defined to the path
   * @param searchedPath the path to a json element
   * @return the interval where to find the element in the text
   * @throws JsonParseException if invalid json
   * @throws IOException if invalid json
   */
  public Interval locatePath(JsonPath searchedPath) throws JsonParseException, IOException {
    path = new JsonPath();
    try (JsonParser p = JSONUtil.mapper().getFactory().createParser(cellText)) {
      parser = p;
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        updatePath(token);
        if (path.equals(searchedPath)) {
          break;
        }
      }
      if (token == null) {
        return null;
      }
      if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
        token = parser.nextToken();
        if (token == null) {
          return null;
        }
        updatePath(token);
      }
      int start = (int)parser.getTokenLocation().getCharOffset() + (parser.getCurrentToken() == JsonToken.VALUE_STRING ? 1 : 0);
      switch (parser.getCurrentToken()) {
      case VALUE_STRING: // values are one token
      case VALUE_TRUE:
      case VALUE_FALSE:
      case VALUE_NULL:
      case VALUE_EMBEDDED_OBJECT:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT: {
        int end = start + parser.getTextLength();
        return new Interval(start, end);
      }
      case START_OBJECT: // if array or object need to walk till the end of it
      case START_ARRAY:
        while ((token = parser.nextToken()) != null) {
          updatePath(token);
          if (path.equals(searchedPath)) {
            int end = (int)parser.getTokenLocation().getCharOffset() + parser.getTextLength();
            return new Interval(start, end);
          }
        }
        break;
      case END_ARRAY:
      case END_OBJECT:
      case FIELD_NAME:
      case NOT_AVAILABLE:
      default:
        return null;
      }
    }
    return null;
  }

  /**
   * String interval
   */
  public static class Interval {
    private final int start;
    private final int end;
    public Interval(int start, int end) {
      super();
      this.start = start;
      this.end = end;
    }
    public int getStart() {
      return start;
    }
    public int getEnd() {
      return end;
    }
    @Override
    public String toString() {
      return "Interval[" + start + ", " + end + "]";
    }
  }

  /**
   * @param selectionStart the start index of the element in the cellText passed to the constructor
   * @param selectionEnd the end index of the element in the cellText passed to the constructor
   * @return the path to the element delimited by the provided interval
   * @throws JsonParseException if invalid json
   * @throws IOException if invalid json
   */
  public JsonSelection locate(int selectionStart, int selectionEnd) throws JsonParseException, IOException {
    path = new JsonPath();
    JsonPath start = null;
    JsonPath end = null;
    int previousEnd = -1;
    try (JsonParser p = JSONUtil.mapper().getFactory().createParser(cellText)) {
      parser = p;
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        int currentStart = (int)parser.getTokenLocation().getCharOffset();
        int currentEnd = currentStart + parser.getTextLength();
        if (previousEnd < selectionEnd && selectionEnd < currentStart) {
          if (end != null) {
            throw new IllegalStateException(token + " " + path + " " + end);
          }
          end = new JsonPath(path);
        }
        //      System.out.println(String.format("t: %s %s, pe: %d, s: %d, e: %d, ss: %d, se: %d", token, parser.getText(), previousEnd, currentStart, currentEnd, selectionStart, selectionEnd));
        updatePath(token);
        //      System.out.println(path);
        if ((previousEnd < selectionStart || currentStart <= selectionStart) && selectionStart < currentEnd) {
          if (start != null) {
            throw new IllegalStateException(token + " " + path + " " + start);
          }
          start = new JsonPath(path);
        }
        if (currentStart <= selectionEnd && selectionEnd <= currentEnd) {
          if (end != null) {
            throw new IllegalStateException(token + " " + path + " " + end);
          }
          end = new JsonPath(path);
        }
        previousEnd = currentEnd;
      }
      if (start == null) {
        start = new JsonPath(path);
      }
      if (end == null) {
        end = new JsonPath(path);
      }
    }
//    System.out.println("start: " + start);
//    System.out.println("end: " + end);
    return new JsonSelection(start, end);
  }

  private void updatePath(JsonToken token) throws IOException {
    switch (token) {
    case START_ARRAY:
      inc();
      pushArray();
      break;
    case END_ARRAY:
      popArray();
      break;
    case START_OBJECT:
      inc();
      pushObject(null);
      break;
    case END_OBJECT:
      popObject();
      break;
    case FIELD_NAME:
      popObject();
      currentField = parser.getText();
      pushObject(currentField);
      break;
    case VALUE_EMBEDDED_OBJECT:
    case VALUE_FALSE:
    case VALUE_NULL:
    case VALUE_NUMBER_FLOAT:
    case VALUE_NUMBER_INT:
    case VALUE_STRING:
    case VALUE_TRUE:
      inc();
      break;
    case NOT_AVAILABLE:
    default:
      throw new IllegalStateException(token + " " + path);
    }
  }

  /**
   * selection interval in json elements
   */
  public static class JsonSelection {
    private final JSONElementLocator.JsonPath start;
    private final JSONElementLocator.JsonPath end;
    public JsonSelection(JSONElementLocator.JsonPath start, JSONElementLocator.JsonPath end) {
      super();
      this.start = start;
      this.end = end;
    }

    public JsonPath getStart() {
      return start;
    }

    public JsonPath getEnd() {
      return end;
    }

    @Override
    public String toString() {
      return String.format("Selection[s:%s, e:%s]", start, end);
    }
  }

  /**
   * json path for extraction
   */
  public static class JsonPath implements Iterable<JsonPathElement> {
    private final Deque<JsonPathElement> path;

    public JsonPath(String path) {
      this.path = parseNextPathElement(path, 0);
    }

    public JsonPath(JsonPathElement... elements) {
      List<JsonPathElement> l = new ArrayList<>(asList(elements));
      Collections.reverse(l);
      this.path = new ArrayDeque<JsonPathElement>(l);
    }

    private JsonPath() {
      this.path = new ArrayDeque<JsonPathElement>();
    }

    private JsonPath(JsonPath other) {
      this.path = new ArrayDeque<JsonPathElement>(other.path);
      // remove irrelevant object start without field dereference
      if (last().isObject() && last().asObject().getField() == null) {
        path.pop();
      }
    }

    private Deque<JsonPathElement> parseNextPathElement(String path, int index) {
      if (index == path.length()) {
        return new ArrayDeque<JsonPathElement>();
      }
      if (path.charAt(index) == '.') {
        if (index + 1 == path.length()) {
          throw new IllegalArgumentException(path + " should not end with .");
        }
        int a = path.indexOf('[', index + 1);
        int d = path.indexOf('.', index + 1);
        int i;
        if (a >= 0 && (a < d || d < 0)) {
          i = a;
        } else if (d >= 0 && (d < a  || a < 0)) {
          i = d;
        } else {
          i = path.length();
        }
        Deque<JsonPathElement> next = parseNextPathElement(path, i);
        next.add(new JSONElementLocator.ObjectJsonPathElement(path.substring(index + 1, i)));
        return next;
      } else if (path.charAt(index) == '[') {
        int a = index + 1 == path.length() ? -1 : path.indexOf(']', index + 1);
        if (a == -1) {
          throw new IllegalArgumentException(path + " missing ]");
        }
        Deque<JsonPathElement> next = parseNextPathElement(path, a + 1);
        next.add(new JSONElementLocator.ArrayJsonPathElement(Integer.parseInt(path.substring(index + 1, a))));
        return next;
      } else {
        throw new IllegalArgumentException(path + " should start with . or [");
      }
    }

    public int size() {
      return path.size();
    }

    public JsonPathElement last() {
      return path.peek();
    }

    public Iterator<JsonPathElement> iterator() {
      return path.descendingIterator();
    }

    public String toString() {
      return Joiner.on("").join(path.descendingIterator());
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      JsonPath other = (JsonPath) obj;
      Iterator<JsonPathElement> i1 = path.iterator();
      Iterator<JsonPathElement> i2 = other.path.iterator();
      for (; i2.hasNext() && i1.hasNext();) {
        if (!i1.next().equals(i2.next())) {
          return false;
        }
      }
      return !(i1.hasNext() || i2.hasNext());
    }

  }

  /**
   * base class of json path elements
   */
  public abstract static class JsonPathElement {
    public abstract boolean isArray();
    public abstract boolean isObject();

    public ArrayJsonPathElement asArray() {
      return (ArrayJsonPathElement) this;
    }
    public ObjectJsonPathElement asObject() {
      return (ObjectJsonPathElement) this;
    }
  }

  /**
   * [x]
   */
  public static class ArrayJsonPathElement extends JsonPathElement {
    private final int position;
    private final ArrayJsonPathElement parent;
    private int count;

    public ArrayJsonPathElement(int position) {
      this.position = position;
      this.count = position;
      this.parent = null;
    }

    public ArrayJsonPathElement() {
      this.position = -1;
      this.count = 0;
      this.parent = null;
    }
    private ArrayJsonPathElement(ArrayJsonPathElement parent) {
      this.position = parent.position + 1;
      this.parent = parent;
      this.count = parent.count + 1;
      incParents();
    }
    private void incParents() {
      if (parent != null) {
        parent.count += 1;
        parent.incParents();
      }
    }
    @Override
    public boolean isArray() {
      return true;
    }
    @Override
    public boolean isObject() {
      return false;
    }
    public int getPosition() {
      return position;
    }
    public ArrayJsonPathElement inc() {
      return new ArrayJsonPathElement(this);
    }
    public int getCount() {
      return count;
    }
    @Override
    public String toString() {
      return "[" + position + "]";
    }
    @Override
    public int hashCode() {
      return position;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      ArrayJsonPathElement other = (ArrayJsonPathElement) obj;
      return other.position == position;
    }
  }

  /**
   * .x
   *
   */
  public static class ObjectJsonPathElement extends JsonPathElement{
    private String field;
    public ObjectJsonPathElement(String field) {
      this.field = field;
    }
    @Override
    public boolean isArray() {
      return false;
    }
    @Override
    public boolean isObject() {
      return true;
    }
    public String getField() {
      return field;
    }
    @Override
    public String toString() {
      return "." + field;
    }
    @Override
    public int hashCode() {
      return field == null ? 0 : field.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      ObjectJsonPathElement other = (ObjectJsonPathElement) obj;
      return Objects.equal(other.field, field);
    }

  }
}
