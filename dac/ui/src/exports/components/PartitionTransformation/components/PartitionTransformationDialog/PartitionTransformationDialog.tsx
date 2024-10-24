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

import { Controller, useForm } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import {
  PartitionTransformations,
  ReflectionDataType,
} from "dremio-ui-common/sonar/reflections/ReflectionDataTypes.js";
import { Button, DialogContent, IconButton } from "dremio-ui-lib/components";
import { NumberInput } from "@mantine/core";
import { zodResolver } from "@hookform/resolvers/zod";
import { PartitionTransformationSchema } from "#oss/exports/components/PartitionTransformation/components/PartitionTransformationDialog/PartitionTransformationSchema";
import {
  PARTITION_TRANSFORMATION_LOWER_LIMIT,
  PARTITION_TRANSFORMATION_UPPER_LIMIT,
} from "#oss/exports/components/PartitionTransformation/PartitionTransformationUtils";
import { typeToIconType } from "#oss/constants/DataTypes";

type PartitionTransformationDialogProps = {
  columnName: string;
  columnType: ReflectionDataType;
  transformationType:
    | PartitionTransformations.BUCKET
    | PartitionTransformations.TRUNCATE;
  defaultValue?: number;
  setPartitionTransformation: (
    transformType: PartitionTransformations,
    count?: number,
  ) => void;
  modalContainer: {
    open: () => void;
    close: () => void;
    isOpen: boolean;
  };
};

function PartitionTransformationDialog({
  columnName,
  columnType,
  transformationType,
  defaultValue,
  setPartitionTransformation,
  modalContainer,
}: PartitionTransformationDialogProps) {
  const {
    control,
    handleSubmit,
    formState: { isValid },
  } = useForm({
    resolver: zodResolver(PartitionTransformationSchema),
    mode: "onChange",
  });

  const onSubmit = (data: { numberInput: number }) => {
    setPartitionTransformation(transformationType, data.numberInput);
  };

  return (
    <form
      style={{ display: "contents" }}
      onSubmit={(e) => {
        e.stopPropagation();
        handleSubmit(onSubmit)(e);
        modalContainer.close();
      }}
    >
      <DialogContent
        title={
          <>
            <FormattedMessage
              id={`Acceleration.PartitionTransformation.${
                transformationType === PartitionTransformations.BUCKET
                  ? "Bucket"
                  : "Truncate"
              }`}
            />
            <dremio-icon
              class="icon-primary"
              name={`data-types/${typeToIconType[columnType]}`}
            ></dremio-icon>
            <span>{columnName}</span>
          </>
        }
        toolbar={
          <IconButton aria-label="Close" onClick={modalContainer.close}>
            <dremio-icon name="interface/close-small" />
          </IconButton>
        }
        actions={
          <>
            <Button variant="secondary" onClick={modalContainer.close}>
              <FormattedMessage id="Common.Close" />
            </Button>
            <Button variant="primary" type="submit" disabled={!isValid}>
              <FormattedMessage id="Common.Done" />
            </Button>
          </>
        }
        className="transformation-dialog"
      >
        <div style={{ minWidth: "568px" }}>
          <div style={{ marginBottom: "24px" }}>
            {transformationType === PartitionTransformations.BUCKET ? (
              <FormattedMessage id="Acceleration.PartitionTransformation.BucketDialog.Description" />
            ) : (
              <FormattedMessage id="Acceleration.PartitionTransformation.TruncateDialog.Description" />
            )}
          </div>
          <Controller
            name="numberInput"
            control={control}
            render={({ field, fieldState: { error } }) => (
              <NumberInput
                label={
                  transformationType === PartitionTransformations.BUCKET ? (
                    <FormattedMessage id="Acceleration.PartitionTransformation.BucketDialog.Label" />
                  ) : (
                    <FormattedMessage id="Acceleration.PartitionTransformation.TruncateDialog.Label" />
                  )
                }
                defaultValue={defaultValue}
                min={PARTITION_TRANSFORMATION_LOWER_LIMIT}
                max={PARTITION_TRANSFORMATION_UPPER_LIMIT}
                error={error?.message}
                {...field}
              />
            )}
          />
        </div>
      </DialogContent>
    </form>
  );
}

export default PartitionTransformationDialog;
