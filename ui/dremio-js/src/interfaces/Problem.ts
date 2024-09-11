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

const problemPrefix = "https://api.dremio.dev/problems/" as const;

type ProblemType = `${typeof problemPrefix}${string}`;
type ValidationProblemType = `${typeof problemPrefix}validation/${string}`;

export interface Problem {
  /**
   * A human-readable explanation specific to this occurrence of the problem
   */
  readonly detail?: string;

  /**
   * Additional information specific to this type of problem
   */
  readonly meta?: Record<string, any>;

  /**
   * A human-readable summary of the problem type
   */
  readonly title?: string;

  /**
   *  A URI reference that uniquely identifies the problem type
   */
  readonly type: ProblemType;
}

export interface ValidationProblem {
  readonly errors: {
    readonly detail?: string;
    readonly pointer: string;
    readonly type: ValidationProblemType;
  }[];
  readonly title: "There was a problem validating the content of the request";
  readonly type: "https://api.dremio.dev/problems/validation-problem";
}

/**
 * Resolves an `unknown` object type into a `Problem` if it has
 * the required `type` field set with the proper prefix
 *
 * @internal
 * @hidden
 */
export const isProblem = (object: unknown): object is Problem => {
  return (
    object !== null &&
    typeof object === "object" &&
    typeof (object as Record<string, any>)["type"] === "string" &&
    (object as Record<string, any>)["type"].startsWith(problemPrefix)
  );
};
