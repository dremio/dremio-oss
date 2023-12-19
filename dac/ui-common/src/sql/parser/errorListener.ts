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
  CommonToken,
  RecognitionException,
  Recognizer,
} from "antlr4ts";
import { Logger } from "../../contexts/LoggingContext";

export type QueryParseError = {
  type: "lexer" | "parser";
  e: RecognitionException | undefined;
  msg: string;
  offendingSymbol: CommonToken | undefined;
  line: number;
  charPositionInLine: number;
};

export type ErrorListener = ANTLRErrorListener<CommonToken>;

export function createParserErrorListeners(logger: ReturnType<Logger>): {
  getErrors: () => QueryParseError[];
  lexerErrorListener: ErrorListener;
  parserErrorListener: ErrorListener;
} {
  const errors: QueryParseError[] = [];
  const lexerErrorListener = {
    syntaxError: (
      _recognizer: Recognizer<CommonToken, any>,
      offendingSymbol: CommonToken | undefined,
      line: number,
      charPositionInLine: number,
      msg: string,
      e: RecognitionException | undefined
    ) => {
      errors.push({
        type: "lexer",
        e,
        msg,
        offendingSymbol,
        line,
        charPositionInLine,
      });
      logger.error(`Error lexing query.
Offending symbol: ${offendingSymbol}
Line: ${line}
Char position in line: ${charPositionInLine}
Msg: ${msg}`);
    },
  };
  const parserErrorListener = {
    syntaxError: (
      _recognizer: Recognizer<CommonToken, any>,
      offendingSymbol: CommonToken | undefined,
      line: number,
      charPositionInLine: number,
      msg: string,
      e: RecognitionException | undefined
    ) => {
      errors.push({
        type: "parser",
        e,
        msg,
        offendingSymbol,
        line,
        charPositionInLine,
      });
      logger.error(`Error parsing query.
Offending symbol: ${offendingSymbol}
Line: ${line}
Char position in line: ${charPositionInLine}
Msg: ${msg}`);
    },
  };
  return { getErrors: () => errors, lexerErrorListener, parserErrorListener };
}

export const noOpLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
} as const;
