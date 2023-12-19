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

import { ARCTIC, NESSIE } from "@app/constants/sourceTypes";

export function sourceTypesIncludeS3(sourceTypes: { sourceType: string }[]) {
  return sourceTypes && !!sourceTypes.find((type) => type.sourceType === "S3");
}

export function sourceTypesIncludeSampleSource(
  sourceTypes: { sourceType: string }[]
) {
  return (
    sourceTypes &&
    !!sourceTypes.find((type) => type.sourceType === "SAMPLE_SOURCE")
  );
}

export function isVersionedSource(type: string) {
  switch (type) {
    case NESSIE:
    case ARCTIC:
      return true;
    default:
      return false;
  }
}

export function isArcticSource(type?: string) {
  return type === ARCTIC;
}

export function isNessieSource(type?: string) {
  return type === NESSIE;
}
