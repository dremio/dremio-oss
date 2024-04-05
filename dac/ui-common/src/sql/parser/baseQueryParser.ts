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
  ANTLRErrorListener,
  CharStream,
  CharStreams,
  CommonTokenStream,
  Lexer,
  Parser,
  ParserRuleContext,
} from "antlr4ts";

const TAB_SIZE_SPACES = 4;

export abstract class BaseQueryParser<
  P extends Parser,
  L extends Lexer,
  C extends ParserRuleContext,
> {
  protected tokenStream: CommonTokenStream;
  protected parser: P;
  protected lexer: L;

  constructor(
    query: string,
    lexerErrorListener?: ANTLRErrorListener<any>,
    parserErrorListener?: ANTLRErrorListener<any>,
    omitDefaultErrorListeners?: boolean,
  ) {
    const sql = query.replace("\t", " ".repeat(TAB_SIZE_SPACES));
    const inputStream = CharStreams.fromString(sql);

    this.lexer = this.createLexer(inputStream);
    this.tokenStream = new CommonTokenStream(this.lexer);
    this.parser = this.createParser();

    if (omitDefaultErrorListeners) {
      this.lexer.removeErrorListeners();
      this.parser.removeErrorListeners();
    }
    if (lexerErrorListener) {
      this.lexer.addErrorListener(lexerErrorListener);
    }
    if (parserErrorListener) {
      this.parser.addErrorListener(parserErrorListener);
    }
  }

  public getParser(): P {
    return this.parser;
  }

  public getLexer(): L {
    return this.lexer;
  }

  public abstract parse(): {
    parseTree: C;
    tokenStream: CommonTokenStream;
    parser: P;
    lexer: L;
  };

  protected abstract createParser(): P;

  protected abstract createLexer(inputStream: CharStream): L;
}
