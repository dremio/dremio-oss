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

import React, { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import {
  DataTypeTransformations,
  PartitionTransformations,
  ReflectionDataType,
} from "dremio-ui-common/sonar/reflections/ReflectionDataTypes.js";
import { ModalContainer, useModalContainer } from "dremio-ui-lib/components";
import { Tooltip } from "dremio-ui-lib";
import {
  MenuOptions,
  PartitionTransformationFormat,
} from "#oss/exports/components/PartitionTransformation/PartitionTransformationUtils";
import PartitionTransformationDialog from "#oss/exports/components/PartitionTransformation/components/PartitionTransformationDialog/PartitionTransformationDialog";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import "./PartitionTransformationMenu.less";

type PartitionTransformationMenuProps = {
  currentRow: {
    name: string;
    type: {
      name: ReflectionDataType;
    };
  };
  selectedField?: {
    name:
      | string
      | {
          value: PartitionTransformationFormat;
        };
  };
  granularity?: { code: "O"; alt: "Original" } | { code: "D"; alt: "Date" };
  setPartitionTransformation: (
    transformType: PartitionTransformations,
    count?: number,
  ) => void;
  isRecommendation: boolean;
};

function PartitionTransformationMenu({
  currentRow,
  selectedField,
  granularity,
  setPartitionTransformation,
  isRecommendation,
}: PartitionTransformationMenuProps) {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);
  const bucketModal = useModalContainer();
  const truncateModal = useModalContainer();
  const columnName = currentRow.name;
  const columnType = currentRow.type.name;
  const currentTransformationData = selectedField?.name.value?.transform;
  const currentTransformationType = currentTransformationData?.type;
  const currentTransformationSize =
    currentTransformationData?.bucketTransform?.bucketCount ??
    currentTransformationData?.truncateTransform?.truncateLength;
  const granularityOptions = granularity
    ? granularity.alt === "Original"
      ? ReflectionDataType.TIMESTAMP
      : ReflectionDataType.DATE
    : undefined;

  const handleOpen = (e: React.MouseEvent<HTMLDivElement>) => {
    e.preventDefault();
    setAnchorEl(e.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const menuOptions = useMemo(
    () =>
      DataTypeTransformations[granularityOptions ?? columnType].map(
        (transformOption: PartitionTransformations) => (
          <MenuItem
            selected={
              transformOption === currentTransformationType ||
              (transformOption === PartitionTransformations.IDENTITY &&
                !currentTransformationType)
            }
            onClick={() => {
              handleClose();

              if (transformOption === PartitionTransformations.BUCKET) {
                bucketModal.open();
              } else if (
                transformOption === PartitionTransformations.TRUNCATE
              ) {
                truncateModal.open();
              } else {
                setPartitionTransformation(transformOption);
              }
            }}
            key={transformOption}
            disabled={isRecommendation}
          >
            <FormattedMessage id={MenuOptions[transformOption]} />
            {transformOption === currentTransformationType &&
              [
                PartitionTransformations.BUCKET,
                PartitionTransformations.TRUNCATE,
              ].includes(currentTransformationType) && (
                <div className="current-transformation-subtext">
                  <FormattedMessage
                    id={`Acceleration.PartitionTransformation.${
                      currentTransformationType ===
                      PartitionTransformations.BUCKET
                        ? "Bucket"
                        : "Truncate"
                    }Menu`}
                    values={{
                      transformSize: currentTransformationSize,
                    }}
                  />
                </div>
              )}
          </MenuItem>
        ),
      ),
    [
      columnType,
      granularityOptions,
      currentTransformationType,
      currentTransformationSize,
      bucketModal,
      truncateModal,
      setPartitionTransformation,
    ],
  );

  return (
    menuOptions.length > 1 && (
      <>
        <div onClick={handleOpen} className="AccelerationGridSubCell__menuDiv">
          <dremio-icon name="interface/caret-down" />
        </div>
        <Menu
          id="partition-transformation-menu"
          anchorEl={anchorEl}
          open={open}
          onClose={handleClose}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "right",
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "left",
          }}
          container={
            isRecommendation
              ? document.body.querySelector(".dremio-modal-container")
              : document.body
          }
        >
          <header className="MuiMenu-list__header">
            <FormattedMessage id="Acceleration.PartitionTransformation.Title" />
            <Tooltip
              title="Acceleration.PartitionTransformation.MenuTooltip"
              placement="top"
              PopperProps={{
                container: isRecommendation
                  ? document.body.querySelector(".dremio-modal-container")
                  : document.body,
              }}
            >
              <dremio-icon
                name="interface/information"
                class="MuiMenu-list__header--icon"
              />
            </Tooltip>
          </header>
          {menuOptions}
        </Menu>
        <ModalContainer {...bucketModal}>
          <PartitionTransformationDialog
            columnName={columnName}
            columnType={columnType}
            transformationType={PartitionTransformations.BUCKET}
            defaultValue={
              currentTransformationType === PartitionTransformations.BUCKET
                ? currentTransformationSize
                : undefined
            }
            setPartitionTransformation={setPartitionTransformation}
            modalContainer={bucketModal}
          />
        </ModalContainer>
        <ModalContainer {...truncateModal}>
          <PartitionTransformationDialog
            columnName={columnName}
            columnType={columnType}
            transformationType={PartitionTransformations.TRUNCATE}
            defaultValue={
              currentTransformationType === PartitionTransformations.TRUNCATE
                ? currentTransformationSize
                : undefined
            }
            setPartitionTransformation={setPartitionTransformation}
            modalContainer={truncateModal}
          />
        </ModalContainer>
      </>
    )
  );
}

export default PartitionTransformationMenu;
