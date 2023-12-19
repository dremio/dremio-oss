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

import moize from "moize";

import { timed } from "../../../utilities/timed";
import { createParserErrorListeners, noOpLogger } from "../errorListener";
import { LiveEditParseInfo } from "../types/ParseResult";
import { LiveEditQueryParser } from "../liveEditQueryParser";

export class LiveEditParsingEngine {
  constructor() {
    LiveEditQueryParser.warmCache();
  }

  @timed("LiveEditParsingEngine.runImpl")
  private runImpl(linesContent: string[]): LiveEditParseInfo {
    const query = linesContent.join("\n");

    const { getErrors, lexerErrorListener, parserErrorListener } =
      createParserErrorListeners(noOpLogger);
    const { parseTree, tokenStream, parser, lexer } = new LiveEditQueryParser(
      query,
      lexerErrorListener,
      parserErrorListener,
      true
    ).parse();

    return {
      parseTree,
      parser,
      lexer,
      tokenStream,
      errors: getErrors(),
    };
  }

  // We memoize the run so that autocomplete & error detection called from different lifecycles can share the same
  // parse results, given the same query
  run = moize.shallow(this.runImpl);
}
