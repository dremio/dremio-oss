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
  InvalidParamsError,
  ParamError,
} from "dremio-ui-common/errors/InvalidParamsError";
import { type UseFormSetError } from "react-hook-form";
import { JsonPointer } from "json-ptr";

export type FieldValidationMessages = {
  [fieldPointer: string]: {
    [errorCode: string]: (err: ParamError) => JSX.Element | string;
  };
};

export const setHookFormErrorsFromInvalidParamsError =
  (messages: FieldValidationMessages) =>
  (setError: UseFormSetError<any>, err: InvalidParamsError) => {
    err.errors.forEach((error) => {
      const field = JsonPointer.create(error.source.pointer);
      const message =
        messages[error.source.pointer]?.[error.code]?.(error) ||
        error.title ||
        "An unknown error occurred";

      setError(field.path[0] as string, {
        type: "custom",
        message,
      });
    });
  };
