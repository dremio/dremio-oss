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

import { Button, DialogContent } from "dremio-ui-lib/dist-esm";
import { TextInput } from "@mantine/core";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { newArcticCatalogSchema } from "./newArcticCatalogSchema";
import { usePromise } from "react-smart-promise";
import { useEffect, useMemo, useState } from "react";
import { createArcticCatalog } from "../../endpoints/ArcticCatalogs/createArcticCatalog";
import { InvalidParamsError } from "dremio-ui-common/errors/InvalidParamsError";
import { GenericServerSectionMessage } from "../GenericServerSectionMessage/GenericServerSectionMessage";
import {
  setHookFormErrorsFromInvalidParamsError,
  type FieldValidationMessages,
} from "../../utilities/setHookFormErrorsFromInvalidParamsError";
import { ArcticCatalogsResource } from "@app/exports/resources/ArcticCatalogsResource";

const fieldValidationMessages: FieldValidationMessages = {
  "/name": {
    key_exists: (err) => (
      <>The Arctic catalog “{err.meta.provided}” already exists.</>
    ),
  },
};

export const NewArcticCatalogDialog = (props) => {
  const { onCancel, onSuccess } = props;

  const [submittedData, setSubmittedData] = useState(null);

  const {
    register,
    handleSubmit,
    formState: { errors, isValid, isDirty },
    setError,
  } = useForm({
    resolver: zodResolver(newArcticCatalogSchema),
    mode: "onChange",
  });

  const [err, createdCatalog, createStatus] = usePromise(
    useMemo(() => {
      if (!submittedData) {
        return null;
      }
      return () => {
        return createArcticCatalog(submittedData);
      };
    }, [submittedData])
  );
  const onSubmit = (data) => {
    setSubmittedData(data);
  };
  const submitPending = createStatus === "PENDING";
  const fieldDisabled = submitPending || createStatus === "SUCCESS";
  useEffect(() => {
    if (createStatus === "ERROR" && err instanceof InvalidParamsError) {
      setHookFormErrorsFromInvalidParamsError(fieldValidationMessages)(
        setError,
        err
      );
    }
  }, [err, createStatus, setError]);
  useEffect(() => {
    if (createStatus === "SUCCESS") {
      // eslint-disable-next-line promise/catch-or-return
      ArcticCatalogsResource.fetch().finally(() => {
        onSuccess(createdCatalog);
      });
    }
  }, [onSuccess, createStatus, createdCatalog]);
  return (
    <form
      style={{ display: "contents" }}
      onSubmit={(e) => {
        e.stopPropagation();
        handleSubmit(onSubmit)(e);
      }}
    >
      <DialogContent
        error={
          err &&
          !(err instanceof InvalidParamsError) && (
            <GenericServerSectionMessage
              error={err}
              style={{ maxWidth: "70ch" }}
              title="An unexpected error occurred and the Arctic catalog could not be created."
            />
          )
        }
        title="New Arctic Catalog"
        actions={
          <>
            <Button
              variant="secondary"
              onClick={onCancel}
              disabled={submitPending}
              success={createStatus === "SUCCESS"}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              type="submit"
              pending={submitPending}
              success={createStatus === "SUCCESS"}
              disabled={!isValid || !isDirty}
            >
              Add
            </Button>
          </>
        }
      >
        <div style={{ minHeight: "175px", maxWidth: "70ch" }}>
          <TextInput
            label="Catalog Name"
            description="Enter a unique name that will be easy for your users to reference. This name cannot be edited once it is created."
            error={errors.name?.message}
            disabled={fieldDisabled}
            {...register("name")}
          />
        </div>
      </DialogContent>
    </form>
  );
};
