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
  BailErrorStrategy,
  CharStreams,
  CommonTokenStream,
  RecognitionException,
  Token,
} from "antlr4ts";
import { PredictionMode } from "antlr4ts/atn/PredictionMode";
import type { ParseTree } from "antlr4ts/tree/ParseTree";
import { ParseTreeWalker } from "antlr4ts/tree/ParseTreeWalker";
import { AbstractSQLLexer } from "../../../target/generated-sources/antlr/AbstractSQLLexer";
import { AbstractSQLParser } from "../../../target/generated-sources/antlr/AbstractSQLParser";
import { getLoggingContext, Logger } from "../../contexts/LoggingContext";
import { createParserErrorListeners } from "../parser/errorListener";
import { FullQueryParser } from "../parser/fullQueryParser";
import {
  AbstractSqlToken,
  AbstractSqlGenerator,
  OriginalTokensInfo,
} from "./abstractSqlGenerator";
import { RiverLeftAlignFormatter } from "./riverLeftAlignFormatter";

let _LOGGER: ReturnType<Logger>;
const getLogger = () => {
  if (typeof _LOGGER === "undefined") {
    _LOGGER = getLoggingContext().createLogger("SqlFormatter");
  }
  return _LOGGER;
};

/**
 * Converts a sql query to a formatted sql query.
 */
export function formatQuery(query: string): string {
  const { getErrors, lexerErrorListener, parserErrorListener } =
    createParserErrorListeners(getLogger());
  const { parseTree: queryParseTree, tokenStream: queryTokenStream } =
    new FullQueryParser(query, lexerErrorListener, parserErrorListener).parse();
  const errors = getErrors();

  const firstLexerError = errors.find((error) => error.type == "lexer");
  if (firstLexerError) {
    throw firstLexerError.e || new Error(firstLexerError.msg);
  }

  const firstParserError = errors.find((error) => error.type == "parser");
  if (firstParserError) {
    throw firstParserError.e || new Error(firstParserError.msg);
  }

  const [astTokens, originalTokensInfo] = createAbstractSqlTokens(
    queryParseTree,
    queryTokenStream
  );
  const ast = createAST(astTokens);
  const formatter = new RiverLeftAlignFormatter(originalTokensInfo);
  const result = ast.accept(formatter);
  return result.sql[0];
}

function createAbstractSqlTokens(
  queryParseTree: ParseTree,
  queryTokenStream: CommonTokenStream
): [AbstractSqlToken[], OriginalTokensInfo] {
  const converter = new AbstractSqlGenerator(queryTokenStream);
  ParseTreeWalker.DEFAULT.walk(converter, queryParseTree);
  return converter.getAbstractSqlTokens();
}

function createAST(astTokens: AbstractSqlToken[]): ParseTree {
  const abstractSql: string = createAbstractSql(astTokens);
  const astGeneratorParser = createASTGeneratorParser(abstractSql);
  try {
    return astGeneratorParser.start();
  } catch (e: any) {
    if (e.cause && e.cause instanceof RecognitionException) {
      const re: RecognitionException = e.cause;
      const offendingToken: Token | undefined = re.getOffendingToken();
      getLogger().error(`Error parsing abstract SQL to AST.
Offending symbol text: ${offendingToken?.text}
Offending state: ${re.offendingState}
Token index: ${offendingToken?.tokenIndex}
Msg: ${re.message}
Expected tokens: ${re.expectedTokens}
Stack: ${re.stack}`);
      // Throw the wrapped RecognitionException
      throw e.cause;
    }
    getLogger().error(e.message + "\n" + e.stack);
    throw e;
  }
}

function createAbstractSql(abstractSqlTokens: AbstractSqlToken[]): string {
  return abstractSqlTokens
    .map((token) => {
      const literalName = AbstractSQLLexer.VOCABULARY.getLiteralName(token);
      return literalName?.replace(/'/g, ""); // literal names are 'rulebody'
    })
    .join(" ");
}

function createASTGeneratorParser(text: string): AbstractSQLParser {
  const inputStream = CharStreams.fromString(text);
  const lexer = new AbstractSQLLexer(inputStream);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new AbstractSQLParser(tokenStream);

  // From testing, SLL prediction mode provides a roughly 25x speedup in parsing.
  // E.g. 25 seconds to 1 second for a very complex query - which is still long,
  // because our AST SQL grammar is very ambiguous as it is written to ease formatting
  // at the expense of fast parsing. This mode is documented to not necessarily be able
  // to parse all inputs, but I have not found any such queries that cannot be parsed.
  // If one were found, we might need to add a fallback on ALL prediction mode.
  parser.interpreter.setPredictionMode(PredictionMode.SLL);

  // Unlike the main Dremio SQL parser that uses error listeners, we want to immediately
  // abort rather than try to gracefully recover when parsing the abstract SQL since 1) we know
  // it should succeed since it only runs if the original SQL was parsed successfully, so any
  // parse error indicates an internal bug, and 2) in that bug scenario, the UI window would
  // hang on the order of minutes since the ANTLR error handling that tries to continue parsing
  // while skipping the bad terminal is extremely expensive on our (very ambiguous) grammar.
  parser.errorHandler = new BailErrorStrategy();
  return parser;
}
