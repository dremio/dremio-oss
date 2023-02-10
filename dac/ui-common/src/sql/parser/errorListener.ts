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
import { ANTLRErrorListener, RecognitionException, Recognizer } from "antlr4ts";
import { Logger } from "../../contexts/LoggingContext";

export type QueryParseError = {
  lexer?: {
    e: RecognitionException | undefined;
    msg: string;
  };
  parser?: {
    e: RecognitionException | undefined;
    msg: string;
  };
};

export type ErrorListener = ANTLRErrorListener<number>;

export function createParserErrorListeners(logger: ReturnType<Logger>): {
  error: QueryParseError;
  lexerErrorListener: ErrorListener;
  parserErrorListener: ErrorListener;
} {
  const error: QueryParseError = {};
  const lexerErrorListener = {
    syntaxError: (
      _recognizer: Recognizer<number, any>,
      offendingSymbol: number | undefined,
      line: number,
      charPositionInLine: number,
      msg: string,
      e: RecognitionException | undefined
    ) => {
      // Only keep track of the first one
      if (!error.lexer) {
        error.lexer = { e, msg };
      }
      logger.error(`Error lexing query.
Offending symbol: ${offendingSymbol}
Line: ${line}
Char position in line: ${charPositionInLine}
Msg: ${msg}`);
    },
  };
  const parserErrorListener = {
    syntaxError: (
      _recognizer: Recognizer<number, any>,
      offendingSymbol: number | undefined,
      line: number,
      charPositionInLine: number,
      msg: string,
      e: RecognitionException | undefined
    ) => {
      // Only keep track of the first one
      if (!error.parser) {
        error.parser = { e, msg };
      }
      logger.error(`Error parsing query.
Offending symbol: ${offendingSymbol}
Line: ${line}
Char position in line: ${charPositionInLine}
Msg: ${msg}`);
    },
  };
  return { error, lexerErrorListener, parserErrorListener };
}
