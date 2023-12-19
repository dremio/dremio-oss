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
import type { CharStream, CommonTokenStream } from "antlr4ts";

import {
  LiveEditParser,
  SqlStmtListContext,
} from "../../../target/generated-sources/antlr/LiveEditParser";
import { LiveEditLexer } from "../../../target/generated-sources/antlr/LiveEditLexer";
import { BaseQueryParser } from "./baseQueryParser";
import { createParserErrorListeners, noOpLogger } from "./errorListener";
import { timed } from "../../utilities/timed";

export class LiveEditQueryParser extends BaseQueryParser<
  LiveEditParser,
  LiveEditLexer,
  SqlStmtListContext
> {
  private static cacheWarmed: boolean = false;

  protected createParser(): LiveEditParser {
    return new LiveEditParser(this.tokenStream);
  }

  protected createLexer(inputStream: CharStream): LiveEditLexer {
    const lexer = new LiveEditLexer(inputStream);
    lexer.mode(LiveEditLexer.DQID);
    return lexer;
  }

  @timed("LiveEditQueryParser.parse")
  parse(): {
    parseTree: SqlStmtListContext;
    tokenStream: CommonTokenStream;
    parser: LiveEditParser;
    lexer: LiveEditLexer;
  } {
    const parseTree = this.parser.sqlStmtList();
    return {
      parseTree,
      tokenStream: this.tokenStream,
      parser: this.parser,
      lexer: this.lexer,
    };
  }

  /**
   * The first parse always takes 100ms+ longer due to creating the DFA cache, shared across all instances of the parser
   * This function mitigates that by allowing for pre-warming the cache.
   */
  @timed("LiveEditQueryParser.warmCache")
  static warmCache(): void {
    if (!LiveEditQueryParser.cacheWarmed) {
      LiveEditQueryParser.cacheWarmed = true;
      const { lexerErrorListener, parserErrorListener } =
        createParserErrorListeners(noOpLogger);
      const queryParser = new LiveEditQueryParser(
        "SELECT",
        lexerErrorListener,
        parserErrorListener,
        true
      );
      queryParser.parse();
    }
  }
}
