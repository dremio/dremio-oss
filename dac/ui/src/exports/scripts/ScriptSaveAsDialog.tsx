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

import { isProblem, isValidationProblem } from "@dremio/dremio-js/interfaces";
import { Button, DialogContent } from "dremio-ui-lib/components";
import type { FC } from "react";
import { useForm } from "react-hook-form";
import { ajvResolver } from "@hookform/resolvers/ajv";
import { useSuspenseQuery } from "@tanstack/react-query";
import { scriptQuery, useScriptSaveMutation } from "@inject/queries/scripts";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { stripTemporaryPrefix } from "dremio-ui-common/sonar/SqlRunnerSession/utilities/temporaryTabs.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  FieldValidationMessages,
  setHookFormErrorsFromValidationProblem,
} from "#oss/exports/utilities/setHookFormErrorsFromValidationProblem";
import { SectionMessageErrorFromProblem } from "../components/SectionMessageErrorFromProblem";

export const ScriptSaveAsDialog: FC<{
  close: () => void;
  scriptId: string;
}> = (props) => {
  const pid = getSonarContext().getSelectedProjectId?.();

  const script = useSuspenseQuery(
    scriptQuery(pid)(props.scriptId),
  ).data.unwrap();

  const {
    formState: { errors, isValid },
    handleSubmit,
    register,
    setError,
  } = useForm({
    defaultValues: getDefaultValues(script),
    mode: "onChange",
    resolver: ajvResolver(getSchema()),
  });

  const scriptSaveMutation = useScriptSaveMutation(pid, script, {
    onError: (error: unknown) => {
      if (isValidationProblem(error)) {
        setHookFormErrorsFromValidationProblem(getValidationMessages())(
          setError,
          error,
        );
      }
    },
    onSuccess: () => {
      props.close();
    },
  });

  const { t } = getIntlContext();

  return (
    <form
      onSubmit={handleSubmit((data) => {
        scriptSaveMutation.mutate(data);
      })}
    >
      <DialogContent
        actions={
          <div className="dremio-button-group">
            <Button onClick={() => props.close()} variant="secondary">
              {t("Common.Actions.Cancel")}
            </Button>
            <Button
              disabled={!isValid}
              pending={scriptSaveMutation.isPending}
              type="submit"
              variant="primary"
            >
              {t("Common.Actions.Save")}
            </Button>
          </div>
        }
        title={t("Script.SaveAsDialog.Title")}
      >
        <div className="pb-6" style={{ width: "600px" }}>
          {scriptSaveMutation.isError &&
            isProblem(scriptSaveMutation.error) &&
            !isValidationProblem(scriptSaveMutation.error) && (
              <SectionMessageErrorFromProblem
                className="mb-2"
                problem={scriptSaveMutation.error}
              />
            )}
          <div className="form-group">
            <label>
              {t("Script.SaveAsDialog.Field.Name.Label")}
              <input
                aria-invalid={!!errors.name}
                autoComplete="off"
                autoFocus={true}
                className="mt-05 form-control"
                type="text"
                {...register("name")}
              />
            </label>
            {errors.name && (
              <div
                className="mt-05 text-sm"
                style={{ color: "var(--color--danger--400)" }}
              >
                {errors.name.message}
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </form>
  );
};

const getSchema = () =>
  ({
    type: "object",
    properties: {
      name: {
        type: "string",
        minLength: 1,
        errorMessage: {
          minLength: getIntlContext().t("scripts:validation/min-length:name"),
        },
      },
    },
    required: ["name"],
    additionalProperties: false,
  }) as const;

const getValidationMessages = () => {
  const { t } = getIntlContext();
  return {
    "#/name": {
      "https://api.dremio.dev/problems/validation/field-conflict": (err) =>
        t("scripts:validation/field-conflict:name", {
          name: err.value,
        }),
    },
  } as const satisfies FieldValidationMessages;
};

const getDefaultValues = (script: any) => ({
  name: stripTemporaryPrefix(script.name),
});
