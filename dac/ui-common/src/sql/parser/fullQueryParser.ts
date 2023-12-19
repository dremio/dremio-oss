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
import { CharStream, CommonTokenStream } from "antlr4ts";

import { BaseQueryParser } from "./baseQueryParser";
import { DremioLexer } from "../../../target/generated-sources/antlr/DremioLexer";
import {
  DremioParser,
  SqlStmtListContext,
} from "../../../target/generated-sources/antlr/DremioParser";

export class FullQueryParser extends BaseQueryParser<
  DremioParser,
  DremioLexer,
  SqlStmtListContext
> {
  protected createParser(): DremioParser {
    return new DremioParser(this.tokenStream);
  }

  protected createLexer(inputStream: CharStream): DremioLexer {
    const lexer = new DremioLexer(inputStream);
    lexer.mode(DremioLexer.DQID);
    return lexer;
  }

  parse(): {
    parseTree: SqlStmtListContext;
    tokenStream: CommonTokenStream;
    parser: DremioParser;
    lexer: DremioLexer;
  } {
    const parseTree = this.parser.sqlStmtList();
    return {
      parseTree,
      tokenStream: this.tokenStream,
      parser: this.parser,
      lexer: this.lexer,
    };
  }
}
