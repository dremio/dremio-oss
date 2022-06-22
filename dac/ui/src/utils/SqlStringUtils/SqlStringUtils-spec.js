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

import { SqlStringUtils } from "./SqlStringUtils";

describe("SqlStringUtils", () => {
  it("should return an empty array", () => {
    expect(SqlStringUtils("")[0]).to.deep.equal([]);
  });

  it("should return a simple statement", () => {
    expect(SqlStringUtils("SELECT * FROM FOO")[0]).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
  });

  it("should return a simple statement with a semicolon", () => {
    expect(SqlStringUtils("SELECT * FROM FOO;")[0]).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
  });

  it("should parse two statements", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO; SELECT * FROM BAR")[0]
    ).to.deep.equal(["SELECT * FROM FOO", "SELECT * FROM BAR"]);
  });

  it("should parse two statements that have an ending semicolon", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO; SELECT * FROM BAR;")[0]
    ).to.deep.equal(["SELECT * FROM FOO", "SELECT * FROM BAR"]);
  });

  it("should parse a single statement followed by semicolons", () => {
    expect(SqlStringUtils("SELECT * FROM FOO;;;;;;;;;;")[0]).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
  });

  it("should parse a single statement followed by semicolons", () => {
    expect(
      SqlStringUtils(
        "SELECT 1;;;;   ; ; ; ;;; ; ;;;\n;;;;;\n;;;;     ;;;\n    ;;\n;\n SELECT 2;\
        SELECT 3; \n SELECT 4;"
      )[0]
    ).to.deep.equal(["SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4"]);
  });

  it("should parse two statements with a comment and a new line character", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO;--test\nSELECT * FROM FOO")[0]
    ).to.deep.equal(["SELECT * FROM FOO", "--test\nSELECT * FROM FOO"]);
  });

  it("should parse a single statement followed by a comment", () => {
    expect(SqlStringUtils("SELECT * FROM FOO;\n--test")[0]).to.deep.equal([
      "SELECT * FROM FOO\n--test",
    ]);
  });

  it("should parse statemens ending with a comment", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO; SELECT * FROM FOO\n--test")[0]
    ).to.deep.equal(["SELECT * FROM FOO", "SELECT * FROM FOO\n--test"]);
  });

  it("should parse a single statement followed by a comment and new line characters", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO;\n-- test\n\n\n")[0]
    ).to.deep.equal(["SELECT * FROM FOO\n-- test\n\n\n"]);
  });

  it("should parse a single statement followed by a comment and whitespace characters", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO;\n-- test\n\n\n    \r\n   ")[0]
    ).to.deep.equal(["SELECT * FROM FOO\n-- test\n\n\n    \r\n   "]);
  });

  it("should parse a single statement followed by a multi-line comment and new line characters", () => {
    expect(
      SqlStringUtils("SELECT * FROM FOO;/* \n\n\ntest\n\n\n */\n\n\n")[0]
    ).to.deep.equal(["SELECT * FROM FOO\n/* \n\n\ntest\n\n\n */\n\n\n"]);
  });

  it("should parse two statements with double quotes", () => {
    expect(
      SqlStringUtils('SELECT * FROM "FOO;BAR"; SELECT * FROM BAR;')[0]
    ).to.deep.equal(['SELECT * FROM "FOO;BAR"', "SELECT * FROM BAR"]);
  });

  it("should parse two statements with escaped double quotes", () => {
    expect(
      SqlStringUtils('SELECT * FROM "FOO"";BAR"; SELECT * FROM BAR;')[0]
    ).to.deep.equal(['SELECT * FROM "FOO"";BAR"', "SELECT * FROM BAR"]);
  });

  it("should parse two statements with brackets", () => {
    expect(
      SqlStringUtils("SELECT * FROM [FOO]];BAR]; SELECT * FROM BAR;")[0]
    ).to.deep.equal(["SELECT * FROM [FOO]];BAR]", "SELECT * FROM BAR"]);
  });

  it("should parse statements with a comment", () => {
    expect(
      SqlStringUtils("SELECT * FROM [FOO] -- COMMENT ; TEST")[0]
    ).to.deep.equal(["SELECT * FROM [FOO] -- COMMENT ; TEST"]);
  });

  it("should parse statements with multi-line comments", () => {
    expect(
      SqlStringUtils("SELECT * FROM /** COMMENT ; TEST */ FOO")[0]
    ).to.deep.equal(["SELECT * FROM /** COMMENT ; TEST */ FOO"]);
  });

  it("should parse two statements with comments", () => {
    expect(
      SqlStringUtils(
        "SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1; SELECT * FROM BAR"
      )[0]
    ).to.deep.equal([
      "SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1",
      "SELECT * FROM BAR",
    ]);
  });

  it("should parse a statement surrrounded by comments with a semicolon", () => {
    expect(SqlStringUtils("--test\nselect 1;\n-- test2")[0]).to.deep.equal([
      "--test\nselect 1\n-- test2",
    ]);
  });

  it("should parse a statement surrrounded by comments without a semicolon", () => {
    expect(SqlStringUtils("--test\nselect 1\n-- test2")[0]).to.deep.equal([
      "--test\nselect 1\n-- test2",
    ]);
  });

  it("should parse statements with comments mixed in", () => {
    expect(
      SqlStringUtils("--test\nselect 1;\n-- test2\nselect 2")[0]
    ).to.deep.equal(["--test\nselect 1", "-- test2\nselect 2"]);
  });

  it("should parse a statement surrounded with multi-line comments", () => {
    expect(
      SqlStringUtils(
        "/* this is\na multi-line\ncomment\n*/\nselect 1 /*\nanother\nmulti-line\ncomment.       select 2*/"
      )[0]
    ).to.deep.equal([
      "/* this is\na multi-line\ncomment\n*/\nselect 1 /*\nanother\nmulti-line\ncomment.       select 2*/",
    ]);
  });
});
