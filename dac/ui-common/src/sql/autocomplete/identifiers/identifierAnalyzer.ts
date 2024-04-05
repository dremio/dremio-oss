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

import { isColumn } from "./analyzers/columnAnalyzer";
import { isFolder } from "./analyzers/folderAnalyzer";
import { isGenericContainer } from "./analyzers/genericContainerAnalyzer";
import { isSource } from "./analyzers/sourceAnalyzer";
import { isSpace } from "./analyzers/spaceAnalyzer";
import { isUdf } from "./analyzers/udfAnalyzer";
import { isTable } from "./analyzers/tableAnalyzer";
import type { IdentifierCandidate } from "../types/IdentifierCandidate";
import type { CursorInfo } from "../parsing/cursorInfo";
import { excludeTrailingIdentifiers } from "./identifierUtils";

export enum IdentifierType {
  GENERIC_CONTAINER = "GENERIC_CONTAINER",
  COLUMN = "COLUMN",
  FOLDER = "FOLDER",
  SOURCE = "SOURCE",
  SPACE = "SPACE",
  TABLE = "TABLE",
  UDF = "UDF",
}

export type AnalyzedIdentifier =
  | {
      type: Exclude<IdentifierType, IdentifierType.COLUMN>;
    }
  | {
      type: IdentifierType.COLUMN;
      compoundAllowed: boolean;
    };

export function analyzeIdentifier(
  identifierCandidate: IdentifierCandidate,
  cursorInfo: CursorInfo,
): AnalyzedIdentifier | undefined {
  const priorTerminals = excludeTrailingIdentifiers(cursorInfo.priorTerminals);
  if (isGenericContainer(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.GENERIC_CONTAINER };
  } else if (isColumn(priorTerminals, identifierCandidate)) {
    return {
      type: IdentifierType.COLUMN,
      compoundAllowed:
        identifierCandidate.getIdentifierRuleType() == "compoundIdentifier",
    };
  } else if (isFolder(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.FOLDER };
  } else if (isSource(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.SOURCE };
  } else if (isSpace(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.SPACE };
  } else if (isTable(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.TABLE };
  } else if (isUdf(priorTerminals, identifierCandidate)) {
    return { type: IdentifierType.UDF };
  } else {
    return undefined;
  }
}
