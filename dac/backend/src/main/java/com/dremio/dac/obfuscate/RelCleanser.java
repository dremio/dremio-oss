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
package com.dremio.dac.obfuscate;

import com.dremio.common.utils.PathUtils;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool to redact the identifiers and string literals in a Rel tree This is package-protected and
 * all usages should be via ObfuscationUtils because this does not check whether to obfuscate or
 * not.
 */
class RelCleanser {
  private static final Pattern PROJECT_PATTERN =
      Pattern.compile("(Logical)?Project(Rel|Prel)?\\((?<project>.*)\\)");
  private static final Pattern AGG_PATTERN =
      Pattern.compile("(Logical)?Aggregate(Rel|Prel)?\\(group=\\[\\{.*?\\}\\], (?<aggs>.*)\\)");
  private static final Pattern FILTER_PATTERN =
      Pattern.compile("(Logical)?Filter(Rel|Prel)?\\(condition=\\[(?<condition>.*?)\\]\\)");
  private static final Pattern SCAN_PATTERN =
      Pattern.compile(
          "Scan(Crel|Drel|Rel|Prel)?\\(table=\\[(?<table>.*?)\\], columns=\\[(?<columns>.*?)\\].*?(filters=\\[\\[(?<filter>.*)\\]\\])?\\)");
  private static final Pattern EXPANSION_NODE_PATTERN =
      Pattern.compile("Expansion(Leaf)?Node\\(path=\\[(?<path>.*?)\\]\\)");
  private static final Pattern RECORD_TYPE_PATTERN =
      Pattern.compile("RecordType\\((?<recordType>.*)\\):");
  private static final Pattern WRITER_PATTERN =
      Pattern.compile("WriterCommitter\\(final=\\[(?<finalLocation>.*?)\\]");

  public static String redactRelTree(String relTree) {
    BufferedReader reader = new BufferedReader(new StringReader(relTree));
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);

    String line;
    try {
      while ((line = reader.readLine()) != null) {
        String redactedLine = processRel(line);
        writer.println(redactedLine);
      }
    } catch (IOException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    writer.flush();
    return stringWriter.toString();
  }

  private static String processRel(String rel) {
    Matcher m = PROJECT_PATTERN.matcher(rel);
    Matcher filterMatcher = FILTER_PATTERN.matcher(rel);
    Matcher scanMatcher = SCAN_PATTERN.matcher(rel);
    Matcher expapansionMatcher = EXPANSION_NODE_PATTERN.matcher(rel);
    Matcher aggMatcher = AGG_PATTERN.matcher(rel);
    Matcher writerMatcher = WRITER_PATTERN.matcher(rel);
    String redactedRel;
    if (m.find()) {
      redactedRel = processProject(rel, m);
    } else if (filterMatcher.find()) {
      redactedRel = processFilter(rel, filterMatcher);
    } else if (scanMatcher.find()) {
      redactedRel = processScan(rel, scanMatcher);
    } else if (expapansionMatcher.find()) {
      redactedRel = processExpansionNode(rel, expapansionMatcher);
    } else if (aggMatcher.find()) {
      redactedRel = processAgg(rel, aggMatcher);
    } else if (writerMatcher.find()) {
      redactedRel = processWriter(rel, writerMatcher);
    } else {
      redactedRel = rel;
    }
    Matcher recordTypeMatcher = RECORD_TYPE_PATTERN.matcher(redactedRel);
    if (recordTypeMatcher.find()) {
      String recordType = recordTypeMatcher.group("recordType");
      String redactedRecordtype = processRecordType(recordType);
      redactedRel =
          new StringBuilder(redactedRel)
              .replace(
                  recordTypeMatcher.start("recordType"),
                  recordTypeMatcher.end("recordType"),
                  redactedRecordtype)
              .toString();
    }
    return redactedRel;
  }

  private static String processWriter(String rel, Matcher matcher) {
    if (!ObfuscationUtils.shouldObfuscateFull()) {
      return rel;
    }
    int start = matcher.start("finalLocation");
    int end = matcher.end("finalLocation");
    String redactedFinalLocation = "filepath_" + redactString(matcher.group("finalLocation"));
    return new StringBuilder(rel).replace(start, end, redactedFinalLocation).toString();
  }

  private static String processAgg(String rel, Matcher matcher) {
    int start = matcher.start("aggs");
    int end = matcher.end("aggs");
    String redactedProjects = processProjects(matcher.group("aggs"));
    return new StringBuilder(rel).replace(start, end, redactedProjects).toString();
  }

  private static String processExpansionNode(String rel, Matcher matcher) {
    int start = matcher.start("path");
    int end = matcher.end("path");
    String path = matcher.group("path");
    String redactedPath = processTable(path);
    return new StringBuilder(rel).replace(start, end, redactedPath).toString();
  }

  private static final Pattern LITERAL_PATTERN = Pattern.compile("'(?<literal>[^']*)'");

  private static String processExpression(String expr) {
    String result = expr;
    int idx = 0;
    while (true) {
      Matcher matcher = LITERAL_PATTERN.matcher(result);
      if (!matcher.find(idx)) {
        break;
      }

      int start = matcher.start("literal");
      int end = matcher.end("literal");

      String redacted = redactLiteral(matcher.group("literal"));

      int adjust = redacted.length() - (end - start);

      result = new StringBuilder(result).replace(start, end, redacted).toString();

      idx = end + adjust + 1;
    }
    return result;
  }

  private static final Pattern PARQUET_FILTER_PATTERN =
      Pattern.compile("Filter on `(?<field1>[^`]*)`:.*?`(?<field2>[^`]*)`.*?'(?<cond>[^']*)'");

  private static String processParquetFilter(String filter) {
    String replacement = filter;
    String[] groups = new String[] {"field1", "field2", "cond"};
    boolean[] isField = new boolean[] {true, true, false};

    for (int i = 0; i < 3; i++) {
      String group = groups[i];
      Matcher matcher = PARQUET_FILTER_PATTERN.matcher(replacement);
      if (matcher.find()) {
        int start = matcher.start(group);
        int end = matcher.end(group);
        String s = matcher.group(group);
        replacement =
            new StringBuilder(replacement)
                .replace(start, end, isField[i] ? redactField(s) : redactLiteral(s))
                .toString();
      }
    }

    return replacement;
  }

  private static String processScan(String rel, Matcher matcher) {
    int start = matcher.start("columns");
    int end = matcher.end("columns");
    String redactedColumns = processColumns(matcher.group("columns"));
    String replacement = new StringBuilder(rel).replace(start, end, redactedColumns).toString();
    matcher = SCAN_PATTERN.matcher(replacement);
    if (matcher.find() && matcher.group("filter") != null) {
      String redactedFilter = processParquetFilter(matcher.group("filter"));
      int fStart = matcher.start("filter");
      int fEnd = matcher.end("filter");
      replacement = new StringBuilder(replacement).replace(fStart, fEnd, redactedFilter).toString();
    }
    matcher = SCAN_PATTERN.matcher(replacement);
    if (matcher.find() && matcher.group("table") != null) {
      String redactedTable = processTable(matcher.group("table"));
      int fStart = matcher.start("table");
      int fEnd = matcher.end("table");
      replacement = new StringBuilder(replacement).replace(fStart, fEnd, redactedTable).toString();
    }
    return replacement;
  }

  private static String processTable(String table) {
    List<String> pathElements = PathUtils.parseFullPath(table);
    if (whitelisted.contains(pathElements.get(0))) {
      return table;
    }
    List<String> redactedElements =
        ObfuscationUtils.obfuscate(pathElements, RelCleanser::redactPathElement);
    return PathUtils.constructFullPath(redactedElements);
  }

  private static final Pattern COLUMN_PATTERN = Pattern.compile("`(?<column>[^`]*)`");

  private static String processColumns(String columns) {
    Matcher matcher = COLUMN_PATTERN.matcher(columns);
    List<String> redactedColumns = new ArrayList<>();
    while (matcher.find()) {
      redactedColumns.add(String.format("%s%s%s", "`", redactField(matcher.group("column")), "`"));
    }
    return String.join(", ", redactedColumns);
  }

  private static String processFilter(String rel, Matcher matcher) {
    int start = matcher.start("condition");
    int end = matcher.end("condition");
    String condition = matcher.group("condition");
    String newCondition = processExpression(condition);
    return new StringBuilder(rel).replace(start, end, newCondition).toString();
  }

  private static String processProject(String rel, Matcher matcher) {
    int start = matcher.start("project");
    int end = matcher.end("project");
    String redactedProjects = processProjects(matcher.group("project"));
    return new StringBuilder(rel).replace(start, end, redactedProjects).toString();
  }

  private static final Pattern FIELDTYPE_PATTERN =
      Pattern.compile(
          "(?<type>(VARCHAR|VARBINARY)\\(\\d*\\)|INTEGER|BIGINT|DATE|TIMESTAMP|FLOAT|DOUBLE|BOOLEAN|ANY) (?<name>[^,]*)(, )?");

  private static String processRecordType(String recordType) {
    Matcher matcher = FIELDTYPE_PATTERN.matcher(recordType);
    List<String> fieldTypes = new ArrayList<>();
    while (matcher.find()) {
      String fieldType =
          String.join(" ", matcher.group("type"), redactField(matcher.group("name")));
      fieldTypes.add(fieldType);
    }
    return String.join(", ", fieldTypes);
  }

  private static final Pattern PROJECTS_PATTERN = Pattern.compile("(?<project>.*?=\\[.*?])(, )?");

  private static String processProjects(String projects) {
    Matcher matcher = PROJECTS_PATTERN.matcher(projects);
    List<String> newProjects = new ArrayList<>();
    while (matcher.find()) {
      newProjects.add(processProject(matcher.group("project")));
    }
    return String.join(", ", newProjects);
  }

  private static final Pattern PROJECT_EXPR_PATTERN =
      Pattern.compile("(?<name>.*?)=\\[(?<expr>.*)]");

  private static String processProject(String project) {
    Matcher matcher = PROJECT_EXPR_PATTERN.matcher(project);
    if (matcher.find()) {
      String name = matcher.group("name");
      String expr = matcher.group("expr");
      return String.format("%s=[%s]", redactField(name), processExpression(expr));
    }
    return null;
  }

  private static final Set<String> whitelisted = ImmutableSet.of("__accelerator");

  private static String redactPathElement(String element) {
    if (whitelisted.contains(element.toLowerCase())) {
      return element;
    }

    if (!ObfuscationUtils.shouldObfuscateFull()) {
      return element;
    }

    return redactString(element.toLowerCase());
  }

  public static String redactField(String field) {
    if (!ObfuscationUtils.shouldObfuscateFull()) {
      return field;
    }
    return "field_" + redactString(field.toLowerCase());
  }

  private static String redactLiteral(String id) {
    if (!ObfuscationUtils.shouldObfuscatePartial()) {
      return id;
    }
    return "literal_" + redactString(id);
  }

  // This will redact with no checking whether to redact or not.
  private static String redactString(String string) {
    return Integer.toHexString(string.hashCode());
  }
}
