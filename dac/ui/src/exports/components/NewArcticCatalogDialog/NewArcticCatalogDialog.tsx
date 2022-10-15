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

export const NewArcticCatalogDialog = (props) => {
  const { onCancel, onSuccess } = props;

  const [submittedData, setSubmittedData] = useState(null);

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm({
    resolver: zodResolver(newArcticCatalogSchema),
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
    if (createStatus === "SUCCESS") {
      onSuccess(createdCatalog);
    }
  }, [onSuccess, createStatus, createdCatalog]);
  return (
    <form
      onSubmit={(e) => {
        e.stopPropagation();
        handleSubmit(onSubmit)(e);
      }}
    >
      <DialogContent
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
            >
              Add
            </Button>
          </>
        }
      >
        <div style={{ minHeight: "175px" }}>
          <TextInput
            label="Catalog Name"
            description="Give it a name that’s easy for users to reference. It cannot be changed once it’s created."
            error={errors.name?.message}
            disabled={fieldDisabled}
            {...register("name")}
          />
        </div>
      </DialogContent>
    </form>
  );
};
