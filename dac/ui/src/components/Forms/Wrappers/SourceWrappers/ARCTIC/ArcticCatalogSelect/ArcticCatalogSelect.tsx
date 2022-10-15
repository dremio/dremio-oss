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
import { useContext, useCallback, useMemo, memo } from "react";
import { RequestStatus } from "smart-resource";
import { orderBy } from "lodash";
import { FieldWithError } from "@app/components/Fields";
import { ElementConfig } from "@app/types/Sources/SourceFormTypes";
import { FieldProp } from "redux-form";
import { useArcticCatalogs } from "@app/exports/providers/useArcticCatalogs";
import { Select } from "dremio-ui-lib";
import {
  ModalContainer,
  Skeleton,
  useModalContainer,
} from "dremio-ui-lib/dist-esm";

import { fieldWithError } from "../../../FormWrappers.less";
import { ArcticCatalog } from "@app/exports/endpoints/ArcticCatalogs/ArcticCatalog.type";
import { type SelectChangeEvent } from "@mui/material";
import { NewArcticCatalogDialog } from "@app/exports/components/NewArcticCatalogDialog/NewArcticCatalogDialog";
import { intl } from "@app/utils/intl";
import { FormContext } from "@app/pages/HomePage/components/modals/formContext";
import { ArcticCatalogsResource } from "@app/exports/resources/ArcticCatalogsResource";
import sentryUtil from "@app/utils/sentryUtil";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { ARCTIC_CATALOG_CREATION } from "@app/exports/flags/ARCTIC_CATALOG_CREATION";

import * as classes from "./ArcticCatalogSelect.less";

type ArcticCatalogSelectWrapperProps = {
  elementConfig: ElementConfig;
  fields: any;
  field: FieldProp<string>;
  disabled?: boolean;
  editing?: boolean;
};

type ArcticCatalogSelectProps = {
  onChange?: (value: string | null, name?: string | null) => void;
  value?: string | null;
  placeholder?: string;
};

const NEW_CATALOG_ITEM = "NEW_CATALOG_ITEM";

const Skeletons = new Array(5)
  .fill(null)
  .map((value) => ({ label: <Skeleton width="18ch" />, value }));

function getOptions(
  catalogs: ArcticCatalog[] | null,
  status: RequestStatus,
  enableNewCatalogItem: boolean
) {
  if (status === "pending") {
    return Skeletons;
  } else if (catalogs) {
    const options: Record<string, any>[] = orderBy(catalogs, "name").map(
      (cur) => ({
        name: cur.name,
        label: cur.name,
        value: cur.id,
      })
    );
    if (enableNewCatalogItem) {
      options.push({
        label: (
          <span>
            <dremio-icon name="interface/plus" />{" "}
            <span>
              {intl.formatMessage({ id: "ArcticSource.NewCatalogItem" })}
            </span>
          </span>
        ),
        value: NEW_CATALOG_ITEM,
      });
    }
    return options;
  } else {
    return [
      {
        label: intl.formatMessage({ id: "ArcticSource.ErrorCatalogItem" }),
        value: null,
      },
    ];
  }
}

function ArcticCatalogSelect({
  onChange,
  value,
  placeholder,
}: ArcticCatalogSelectProps) {
  const { editing } = useContext(FormContext);
  const [catalogs, , status] = useArcticCatalogs();
  const newArcticCatalog = useModalContainer();
  const [result] = useFeatureFlag(ARCTIC_CATALOG_CREATION);

  const handleArcticCatalogCreation = useCallback(
    async (createdCatalog: ArcticCatalog) => {
      try {
        await ArcticCatalogsResource.fetch();
        onChange?.(createdCatalog.id, createdCatalog.name);
      } catch (e) {
        sentryUtil.logException(e);
      } finally {
        newArcticCatalog.close();
      }
    },
    [newArcticCatalog, onChange]
  );

  const onSelectChange = useCallback(
    (e: SelectChangeEvent<string>) => {
      const value = e.target.value;
      if (value === NEW_CATALOG_ITEM) {
        newArcticCatalog.open();
      } else if (catalogs) {
        const name = catalogs.find(
          (cur: ArcticCatalog) => cur.id === e.target.value
        )?.name;
        onChange?.(e.target.value, name);
      }
    },
    [onChange, newArcticCatalog, catalogs]
  );

  const options = useMemo(
    () => getOptions(catalogs, status, !!result),
    [catalogs, status, result]
  );

  return (
    <>
      <Select
        disabled={editing}
        classes={{ root: classes["arctic-catalog-select"] }}
        {...(placeholder && {
          renderValue: () => placeholder,
        })}
        value={value}
        onChange={onSelectChange}
        options={options}
      ></Select>
      <ModalContainer {...newArcticCatalog}>
        <NewArcticCatalogDialog
          onCancel={newArcticCatalog.close}
          onSuccess={handleArcticCatalogCreation}
        />
      </ModalContainer>
    </>
  );
}
const ArcticCatalogSelectMemo = memo(ArcticCatalogSelect); //Rerender on hover

function ArcticCatalogSelectWrapper({
  field,
  elementConfig,
  fields,
}: ArcticCatalogSelectWrapperProps) {
  function handleChange(value: string | null, name?: string | null) {
    field.onChange(value);
    fields.name.onChange(name);
  }
  return (
    <FieldWithError
      {...field}
      label={intl.formatMessage({
        id: "ArcticSource.SelectCatalogFormItemLabel",
      })}
      name={elementConfig.getPropName()}
      className={fieldWithError}
    >
      <ArcticCatalogSelectMemo
        value={field.value}
        onChange={handleChange}
        placeholder={
          fields.name.value || (
            <span className={classes["placeholder"]}>
              {intl.formatMessage({
                id: "ArcticSource.SelectCatalogPlaceholder",
              })}
            </span>
          )
        }
      />
    </FieldWithError>
  );
}

export default ArcticCatalogSelectWrapper;
