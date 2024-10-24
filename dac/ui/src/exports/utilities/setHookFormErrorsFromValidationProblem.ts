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

import type { ValidationProblem } from "@dremio/dremio-js/interfaces";
import { type UseFormSetError } from "react-hook-form";
import { JsonPointer } from "json-ptr";

export type FieldValidationMessages = {
  [fieldPointer: string]: {
    [errorCode: string]: (err: ValidationProblem["errors"][number]) => string;
  };
};

export const setHookFormErrorsFromValidationProblem =
  (messages: FieldValidationMessages) =>
  (setError: UseFormSetError<any>, validationProblem: ValidationProblem) => {
    validationProblem.errors.forEach((error) => {
      const field = JsonPointer.create(error.pointer);
      setError(field.path[0] as string, {
        type: "custom",
        message: messages[error.pointer][error.type](error) || error.detail,
      });
    });
  };
