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

import { ATN } from "antlr4ts/atn/ATN";
import { IntervalSet } from "antlr4ts/misc/IntervalSet";
import moize from "moize";

import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import { createParserErrorListeners, noOpLogger } from "../errorListener";
import { LiveEditQueryParser } from "../liveEditQueryParser";

export function isKeyword(tokenType: number): boolean {
  return tokenType < LiveEditParser.UNSIGNED_INTEGER_LITERAL;
}

export function getTokenType(
  tokenText: string
): "reservedKeyword" | "nonReservedKeyword" | "other" {
  if (tokenText.trim() === "") {
    return "other";
  }
  const { getErrors, lexerErrorListener, parserErrorListener } =
    createParserErrorListeners(noOpLogger);
  const queryParser = new LiveEditQueryParser(
    tokenText,
    lexerErrorListener,
    parserErrorListener,
    true
  );
  const parseTree = queryParser.getParser().nonReservedKeyWord();
  if (getErrors().find((error) => error.type === "lexer")) {
    return "other";
  }
  const token = queryParser.getParser().inputStream.get(0);
  if (token.type < 0 || !isKeyword(token.type)) {
    return "other";
  }
  return parseTree.exception ? "reservedKeyword" : "nonReservedKeyword";
}

export const getIdentifierTokens: () => IntervalSet = moize(() => {
  const atn = getATN();
  return atn.getExpectedTokens(
    atn.ruleToStartState[
      LiveEditParser.RULE_identifierSegment
    ].getStateNumber(),
    undefined
  );
});

export const getExpressionTokens: () => IntervalSet = moize(() => {
  const atn = getATN();
  return atn.getExpectedTokens(
    atn.ruleToStartState[LiveEditParser.RULE_expression2b].getStateNumber(),
    undefined
  );
});

function getATN(): ATN {
  const queryParser = new LiveEditQueryParser("", undefined, undefined, true);
  return queryParser.getParser().atn;
}
