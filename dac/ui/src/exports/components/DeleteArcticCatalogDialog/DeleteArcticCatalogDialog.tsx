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

import { Button, DialogContent } from "dremio-ui-lib/components";
import { TextInput } from "@mantine/core";
import { useForm } from "react-hook-form";
import { usePromise } from "react-smart-promise";
import { useEffect, useMemo, useState } from "react";
import { deleteArcticCatalog } from "../../endpoints/ArcticCatalogs/deleteArcticCatalog";
import { InvalidParamsError } from "dremio-ui-common/errors/InvalidParamsError";
import { GenericServerSectionMessage } from "../GenericServerSectionMessage/GenericServerSectionMessage";

type Props = {
  catalogId: string;
  catalogName: string;
  onCancel: () => void;
  onSuccess: () => void;
};

export const DeleteArcticCatalogDialog = (props: Props) => {
  const { onCancel, onSuccess } = props;

  const [submittedCatalogId, setSubmittedCatalogId] = useState<string | null>(
    null
  );

  const {
    register,
    handleSubmit,
    formState: { isValid },
  } = useForm({ mode: "onChange" });

  const [err, , createStatus] = usePromise(
    useMemo(() => {
      if (!submittedCatalogId) {
        return null;
      }
      return () => {
        return deleteArcticCatalog({ catalogId: submittedCatalogId });
      };
    }, [submittedCatalogId])
  );
  const onSubmit = () => {
    setSubmittedCatalogId(props.catalogId);
  };
  const submitPending = createStatus === "PENDING";
  const fieldDisabled = submitPending || createStatus === "SUCCESS";
  useEffect(() => {
    if (createStatus === "SUCCESS") {
      onSuccess();
    }
  }, [onSuccess, createStatus]);
  const errorMessage = (err as any)?.responseBody?.errorMessage;
  return (
    <form
      style={{ display: "contents" }}
      onSubmit={(e) => {
        e.stopPropagation();
        handleSubmit(onSubmit)(e);
      }}
      autoComplete="off"
    >
      <DialogContent
        error={
          err &&
          !(err instanceof InvalidParamsError) && (
            <GenericServerSectionMessage
              error={err}
              title={
                errorMessage ??
                "An unexpected error occurred and the Arctic catalog could not be deleted."
              }
            />
          )
        }
        title={
          <>
            <dremio-icon name="interface/warning"></dremio-icon> Delete{" "}
            {props.catalogName}?
          </>
        }
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
              variant="primary-danger"
              type="submit"
              pending={submitPending}
              success={createStatus === "SUCCESS"}
              disabled={!isValid}
            >
              Delete
            </Button>
          </>
        }
      >
        <div style={{ minHeight: "175px" }} className="dremio-prose">
          <p>
            Deleting an Arctic Catalog is an unrecoverable operation. The list
            of curated branches, tags, folders, views, tables, will be deleted.
            However, the data in tables and view will still exist in your object
            store.
          </p>
          <TextInput
            label={`Enter “${props.catalogName}” to confirm`}
            disabled={fieldDisabled}
            placeholder={props.catalogName}
            {...register("name", {
              validate: (value) => value === props.catalogName,
            })}
          />
        </div>
      </DialogContent>
    </form>
  );
};
