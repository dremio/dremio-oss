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
import { useEffect, useMemo, useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { usePromise } from "react-smart-promise";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormattedMessage } from "react-intl";

import { Button, DialogContent } from "dremio-ui-lib/components";
import { intl } from "@app/utils/intl";
import { InvalidParamsError } from "dremio-ui-common/errors/InvalidParamsError";
import { SonarProjectsResource } from "@app/exports/resources/SonarProjectsResource";
import SonarProjectSelect from "../SonarProjectSelect/SonarProjectSelect";
import { GenericServerSectionMessage } from "@app/exports/components/GenericServerSectionMessage/GenericServerSectionMessage";
import { addToSonarSchema } from "./addToSonarSchema";
import { addCatalogToSonar } from "@app/exports/endpoints/ArcticCatalogs/addCatalogToSonar";
import { ArcticCatalog } from "@app/exports/endpoints/ArcticCatalogs/ArcticCatalog.type";
import {
  type FieldValidationMessages,
  setHookFormErrorsFromInvalidParamsError,
} from "@app/exports/utilities/setHookFormErrorsFromInvalidParamsError";
import { SonarProject } from "@app/exports/endpoints/SonarProjects/listSonarProjects";

import * as classes from "./AddToSonarDialogContent.module.less";

type AddToSonarDialogProps = {
  defaultProjectId?: string;
  catalog: ArcticCatalog;
  onSuccess: (proj: SonarProject) => void;
  onCancel: () => void;
};

const fieldValidationMessages: FieldValidationMessages = {
  "/projectId": {
    key_exists: (err) => (
      <FormattedMessage
        id="ArcticCatalog.AddToSonarProjectDialog.ConflictError"
        values={{ catalogName: err.meta.provided }}
      />
    ),
  },
};

function AddToSonarDialogContent({
  defaultProjectId,
  catalog,
  onSuccess,
  onCancel,
}: AddToSonarDialogProps): JSX.Element {
  const [submittedData, setSubmittedData] = useState<Record<
    string,
    any
  > | null>(null);
  const [sonarProjects] = useResourceSnapshot(SonarProjectsResource);
  const projFetchStatus = useResourceStatus(SonarProjectsResource);

  const curProject = useMemo(
    () =>
      sonarProjects?.find(
        (cur) => cur.id && cur.id === submittedData?.projectId
      ),
    [sonarProjects, submittedData]
  );

  const { handleSubmit, setValue, control, formState, setError } = useForm({
    resolver: zodResolver(addToSonarSchema),
    mode: "onChange",
  });
  const { isValid } = formState;

  useEffect(() => {
    if (projFetchStatus === "initial") SonarProjectsResource.fetch();
  }, [projFetchStatus]);

  useEffect(() => {
    if (!sonarProjects) return;
    sonarProjects.map((proj: Record<string, any>) => {
      if (defaultProjectId && proj.id === defaultProjectId) {
        setValue("projectId", proj.id, { shouldValidate: true });
      }
    });
  }, [sonarProjects, defaultProjectId, setValue]);

  const [err, , createStatus] = usePromise(
    useMemo(() => {
      if (!curProject) {
        return null;
      }

      return () => {
        return addCatalogToSonar({
          project: curProject,
          catalog,
        });
      };
    }, [curProject, catalog])
  );
  const errorMessage: string = useMemo(
    () =>
      // @ts-ignore
      err?.responseBody?.errorMessage ||
      intl.formatMessage({
        id: "ArcticCatalog.AddToSonarProjectDialog.UnexpectedError",
      }),
    [err]
  );

  useEffect(() => {
    if (curProject && createStatus === "SUCCESS") {
      onSuccess(curProject);
    }
  }, [submittedData, createStatus, curProject, onSuccess]);

  const onSubmit = (data) => {
    setSubmittedData(data);
  };

  useEffect(() => {
    if (createStatus === "ERROR" && err instanceof InvalidParamsError) {
      setHookFormErrorsFromInvalidParamsError(fieldValidationMessages)(
        setError,
        err
      );
    }
  }, [err, createStatus, setError]);

  const pending = createStatus === "PENDING";
  const disabled =
    pending || projFetchStatus === "pending" || createStatus === "SUCCESS";

  return (
    <form
      style={{ display: "contents" }}
      onSubmit={(e) => {
        e.stopPropagation();
        handleSubmit(onSubmit)(e);
      }}
    >
      <DialogContent
        className={classes["add-to-sonar-dialog-content"]}
        title={intl.formatMessage({
          id: "ArcticCatalog.AddToSonarProjectDialog.Title",
        })}
        actions={
          <>
            <Button
              variant="secondary"
              onClick={onCancel}
              disabled={disabled}
              success={createStatus === "SUCCESS"}
            >
              <FormattedMessage id="Common.Cancel" />
            </Button>
            <Button
              type="submit"
              variant="primary"
              disabled={disabled || !isValid}
              pending={pending}
            >
              <FormattedMessage id="Common.Add" />
            </Button>
          </>
        }
        error={
          err &&
          !(err instanceof InvalidParamsError) && (
            <GenericServerSectionMessage
              error={err}
              style={{ maxWidth: "70ch" }}
              title={errorMessage}
            />
          )
        }
      >
        <Controller
          name="projectId"
          control={control}
          render={({ field }) => (
            <SonarProjectSelect
              label={intl.formatMessage({
                id: "ArcticCatalog.AddToSonarProjectDialog.Label",
              })}
              status={projFetchStatus}
              projects={sonarProjects}
              disabled={disabled}
              {...field}
              error={formState.errors.projectId?.message}
              defaultProjectId={defaultProjectId}
            />
          )}
        />
        <div className={classes["description"]}>
          {intl.formatMessage({
            id: "ArcticCatalog.AddToSonarProjectDialog.Description",
          })}
        </div>
      </DialogContent>
    </form>
  );
}

export default AddToSonarDialogContent;
