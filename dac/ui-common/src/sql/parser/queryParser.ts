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
import { ANTLRErrorListener, CharStreams, CommonTokenStream } from "antlr4ts";
import { ParseTree } from "antlr4ts/tree/ParseTree";
import { DremioLexer } from "../../../dist-antlr/DremioLexer";
import { DremioParser } from "../../../dist-antlr/DremioParser";

const TAB_SIZE_SPACES = 4;

export function parseQuery(
  query: string,
  lexerErrorListener: ANTLRErrorListener<any>,
  parserErrorListener: ANTLRErrorListener<any>
): [ParseTree, CommonTokenStream] {
  const sql = query.replace("\t", " ".repeat(TAB_SIZE_SPACES));
  const inputStream = CharStreams.fromString(sql);

  const lexer = new DremioLexer(inputStream);
  lexer.mode(DremioLexer.DQID);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new DremioParser(tokenStream);
  lexer.addErrorListener(lexerErrorListener);
  parser.addErrorListener(parserErrorListener);

  const parseTree = parser.sqlStmtList();
  return [parseTree, tokenStream];
}
