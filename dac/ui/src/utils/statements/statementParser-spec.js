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

import {
  extractQueries,
  extractSelections,
  extractStatements,
} from "./statementParser";

describe("statement parser", () => {
  it("should return an empty array for empty string", () => {
    expect(extractStatements("")).to.deep.equal([]);
    expect(extractStatements(null)).to.deep.equal([]);
    expect(extractStatements(undefined)).to.deep.equal([]);
  });

  it("should return a single line simple statement as is", () => {
    expect(extractQueries("SELECT * FROM FOO")).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
    expect(extractSelections("SELECT * FROM FOO")).to.deep.equal([
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 1,
        endColumn: 18,
      },
    ]);
  });

  it("should drop semicolon at the end of a single statement", () => {
    expect(extractQueries("SELECT * FROM FOO;")).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
    expect(extractSelections("SELECT * FROM FOO;")).to.deep.equal([
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 1,
        endColumn: 18,
      },
    ]);
  });

  it("should return last character as a separate statement", () => {
    expect(extractQueries("a;b")).to.deep.equal(["a", "b"]);
  });

  it("should parse two statements", () => {
    expect(extractQueries("SELECT * FROM FOO;SELECT * FROM BAR")).to.deep.equal(
      ["SELECT * FROM FOO", "SELECT * FROM BAR"],
    );
    expect(
      extractSelections("SELECT * FROM FOO;SELECT * FROM BAR"),
    ).to.deep.equal([
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 1,
        endColumn: 18,
      },
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 19,
        endColumn: 36,
      },
    ]);
  });

  it("should parse two statements that have multiple semicolons", () => {
    expect(
      extractQueries(";;;;SELECT * FROM FOO;;;;SELECT * FROM BAR;;;;"),
    ).to.deep.equal(["SELECT * FROM FOO", "SELECT * FROM BAR"]);
    expect(
      extractSelections(";;;;SELECT * FROM FOO;;;;SELECT * FROM BAR;;;;"),
    ).to.deep.equal([
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 5,
        endColumn: 22,
      },
      {
        startLineNumber: 1,
        endLineNumber: 1,
        startColumn: 26,
        endColumn: 43,
      },
    ]);
  });

  it("should parse statements ignoring any empty ones which contain only whitespaces or comments", () => {
    const sql =
      "SELECT 1;;;;  --testme\n ;  /* blah \n\n\n SELECT 1234 */ ; ; ;;; ; ;;;\n;;;;;\n;;;;     ;;;\n    ;;\n;\n SELECT 2;SELECT 3; \n SELECT 4;";
    expect(extractQueries(sql)).to.deep.equal([
      "SELECT 1",
      "\n SELECT 2",
      "SELECT 3",
      " \n SELECT 4",
    ]);
  });

  it("should parse two statements with a comment and a new line character", () => {
    expect(
      extractQueries("SELECT * FROM FOO;--test\nSELECT * FROM FOO"),
    ).to.deep.equal(["SELECT * FROM FOO", "--test\nSELECT * FROM FOO"]);
  });

  it("should parse a single statement and discard a comment if it follows a semicolon", () => {
    expect(extractQueries("SELECT * FROM FOO;\n--test")).to.deep.equal([
      "SELECT * FROM FOO",
    ]);
  });

  it("should parse treat a comment that starts with // as a single line comment", () => {
    expect(
      extractQueries("SELECT * FROM FOO//blah select 1234\n;\n//test"),
    ).to.deep.equal(["SELECT * FROM FOO//blah select 1234\n"]);
  });

  it("should parse statemens ending with a comment", () => {
    expect(
      extractQueries("SELECT * FROM FOO;SELECT * FROM FOO\n--test"),
    ).to.deep.equal(["SELECT * FROM FOO", "SELECT * FROM FOO\n--test"]);
  });

  it("should include all whitespaces into a statement", () => {
    expect(
      extractQueries("SELECT * FROM FOO;    SELECT * FROM FOO   \n\t\t\t   "),
    ).to.deep.equal([
      "SELECT * FROM FOO",
      "    SELECT * FROM FOO   \n\t\t\t   ",
    ]);
  });

  it("should parse a single statement followed by a comment and whitespace characters", () => {
    expect(
      extractQueries("SELECT * FROM FOO\n-- test\n\n\n    \r\n   "),
    ).to.deep.equal(["SELECT * FROM FOO\n-- test\n\n\n    \r\n   "]);
  });

  it("should parse a single statement followed by a multi-line comment and new line characters", () => {
    expect(
      extractQueries("SELECT * FROM FOO\n/* \n\n\ntest\n\n\n */\n\n\n"),
    ).to.deep.equal(["SELECT * FROM FOO\n/* \n\n\ntest\n\n\n */\n\n\n"]);
  });

  it("should ignore statements that only contains single or multiline comments", () => {
    expect(
      extractQueries(
        "/* \n\n\ntest\n\n\n */\n\n\n -- jlsdhfsdljkfhsdjklfhjklsdfh \r\n",
      ),
    ).to.deep.equal([]);
    expect(
      extractQueries(
        "select 123;   /* \n\n\ntest\n\n\n */\n\n\n -- jlsdhfsdljkfhsdjklfhjklsdfh \r\n",
      ),
    ).to.deep.equal(["select 123"]);
  });

  it("should parse two statements with double quotes", () => {
    expect(
      extractQueries(
        'SELECT * FROM "FOO--test;BAR/*ljkhsdlfkhdsf*/"; SELECT * FROM BAR;   ',
      ),
    ).to.deep.equal([
      'SELECT * FROM "FOO--test;BAR/*ljkhsdlfkhdsf*/"',
      " SELECT * FROM BAR",
    ]);
  });

  it("should ignore all other quotes inside of double quotes", () => {
    expect(extractQueries('SELECT * FROM ";\';SELECT[;`;BAR"')).to.deep.equal([
      'SELECT * FROM ";\';SELECT[;`;BAR"',
    ]);
  });

  it("should parse two statements with escaped double quotes", () => {
    expect(
      extractQueries('SELECT * FROM "FOO"";BAR";SELECT * FROM BAR;'),
    ).to.deep.equal(['SELECT * FROM "FOO"";BAR"', "SELECT * FROM BAR"]);
  });

  it("should parse new lines as part of quotes", () => {
    expect(extractQueries('SELECT * FROM "FOO\nBAR";select 1')).to.deep.equal([
      'SELECT * FROM "FOO\nBAR"',
      "select 1",
    ]);
    expect(extractQueries("SELECT * FROM 'FOO\nBAR';select 1")).to.deep.equal([
      "SELECT * FROM 'FOO\nBAR'",
      "select 1",
    ]);
    expect(extractQueries("SELECT * FROM `FOO\nBAR`;select 1")).to.deep.equal([
      "SELECT * FROM `FOO\nBAR`",
      "select 1",
    ]);
    expect(extractQueries("SELECT * FROM [FOO\nBAR];select 1")).to.deep.equal([
      "SELECT * FROM [FOO\nBAR]",
      "select 1",
    ]);
  });

  it("should parse two statements with brackets", () => {
    expect(
      extractQueries("SELECT * FROM [FOO]];BAR];SELECT * FROM BAR;"),
    ).to.deep.equal(["SELECT * FROM [FOO]];BAR]", "SELECT * FROM BAR"]);
  });

  it("should parse statements with a comment", () => {
    expect(
      extractQueries("SELECT * FROM [FOO] -- COMMENT ; TEST"),
    ).to.deep.equal(["SELECT * FROM [FOO] -- COMMENT ; TEST"]);
  });

  it("should parse statements with multi-line comments", () => {
    expect(
      extractQueries("SELECT * FROM /** COMMENT ; TEST */ FOO"),
    ).to.deep.equal(["SELECT * FROM /** COMMENT ; TEST */ FOO"]);
  });

  it("should parse two statements with comments", () => {
    expect(
      extractQueries(
        "SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1;SELECT * FROM BAR",
      ),
    ).to.deep.equal([
      "SELECT * FROM [FOO] -- COMMENT ; TEST\r\n WHERE A = 1",
      "SELECT * FROM BAR",
    ]);
  });

  it("should include any leading and trailing spaces into statements", () => {
    expect(
      extractQueries(
        "    SELECT * FROM BAR    \n       ;   \n\n\n\n\t\t\t;  \t \n\n\t  SELECT 1234     ",
      ),
    ).to.deep.equal([
      "    SELECT * FROM BAR    \n       ",
      "  \t \n\n\t  SELECT 1234     ",
    ]);
  });

  it("should parse a statement surrrounded by comments with a semicolon", () => {
    expect(extractQueries("--test\nselect 1;\n-- test2")).to.deep.equal([
      "--test\nselect 1",
    ]);
  });

  it("should parse a statement surrrounded by comments without a semicolon", () => {
    expect(extractQueries("--test\nselect 1\n-- test2")).to.deep.equal([
      "--test\nselect 1\n-- test2",
    ]);
  });

  it("should parse statements with comments mixed in", () => {
    expect(
      extractQueries("--test\nselect 1;\n-- test2\nselect 2"),
    ).to.deep.equal(["--test\nselect 1", "\n-- test2\nselect 2"]);
  });

  it("should parse a statement surrounded with multi-line comments", () => {
    expect(
      extractQueries(
        "/* this is\na multi-line\ncomment\n*/\nselect 1 /*\nanother\nmulti-line\ncomment.       select 2*/",
      ),
    ).to.deep.equal([
      "/* this is\na multi-line\ncomment\n*/\nselect 1 /*\nanother\nmulti-line\ncomment.       select 2*/",
    ]);
  });
});
