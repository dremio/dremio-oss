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
import { useContext, useCallback, useMemo, memo, forwardRef } from "react";
import { orderBy } from "lodash";
import { FieldWithError } from "@app/components/Fields";
import { ElementConfig } from "@app/types/Sources/SourceFormTypes";
import { FieldProp } from "redux-form";
import { Select, SelectItem } from "@mantine/core";
import {
  ModalContainer,
  Skeleton,
  Spinner,
  useModalContainer,
} from "dremio-ui-lib/components";

import { fieldWithError } from "../../../FormWrappers.less";
import { ArcticCatalog } from "@inject/arctic/endpoints/ArcticCatalogs/ArcticCatalog.type";
import { NewArcticCatalogDialog } from "@inject/arctic/components/NewArcticCatalogDialog/NewArcticCatalogDialog";
import { intl } from "@app/utils/intl";
import { FormContext } from "@app/pages/HomePage/components/modals/formContext";
import sentryUtil from "@app/utils/sentryUtil";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { useArcticCatalogs } from "@inject/arctic/providers/useArcticCatalogs";
import { useSelector } from "react-redux";
import { getSortedSources } from "@app/selectors/home";
import { ARCTIC } from "@app/constants/sourceTypes";

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
  error?: string;
  //Might not need both pending and disabled, keeping both for backwards compat after refactor for now
  disabled?: boolean;
  pending?: boolean;
  catalogs: ArcticCatalog[];
  isOptionDisabled?: (catalog: ArcticCatalog) => boolean;
  onCatalogCreateSuccess?: (catalog: ArcticCatalog) => void;
};

interface ItemProps extends React.ComponentPropsWithoutRef<"div"> {
  label?: string;
  value: string;
}

const NEW_CATALOG_ITEM = "NEW_CATALOG_ITEM";
const ERROR_ITEM = "ERROR_ITEM";
const LOADING_ITEM = "LOADING_ITEM";

const Skeletons = new Array(5)
  .fill(null)
  .map((v, i) => ({ label: LOADING_ITEM, value: `${LOADING_ITEM}-${i}` }));

const ArcticCatalogSelectItem = forwardRef<HTMLDivElement, ItemProps>(
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  ({ label, value, ...others }: ItemProps, ref) => {
    function getContent() {
      if (label === LOADING_ITEM) {
        return <Skeleton width="18ch" />;
      } else if (value === ERROR_ITEM) {
        return (
          <span>
            {intl.formatMessage({ id: "VersionedSource.ErrorCatalogItem" })}
          </span>
        );
      } else if (value === NEW_CATALOG_ITEM) {
        return (
          <span>
            <dremio-icon name="interface/plus" />{" "}
            <span>
              {intl.formatMessage({ id: "VersionedSource.NewCatalogItem" })}
            </span>
          </span>
        );
      } else {
        return label;
      }
    }
    return (
      <div ref={ref} {...others}>
        {getContent()}
      </div>
    );
  },
);

function getOptions(
  catalogs: ArcticCatalog[] | null,
  pending: boolean | undefined,
  isOptionDisabled: ((catalog: ArcticCatalog) => boolean) | undefined,
  canAddCatalog: boolean,
) {
  if (pending) {
    return Skeletons;
  } else if (catalogs) {
    const options: (string | SelectItem)[] = orderBy(catalogs, "name").map(
      (cur) => ({
        value: cur.id,
        label: cur.name,
        disabled: isOptionDisabled?.(cur) || false,
      }),
    );
    if (canAddCatalog) {
      options.push({
        value: NEW_CATALOG_ITEM,
        label: "",
      });
    }
    return options;
  } else {
    return [
      {
        value: ERROR_ITEM,
        label: "",
      },
    ];
  }
}

export function ArcticCatalogSelect({
  onChange,
  value,
  placeholder,
  error,
  disabled,
  pending,
  catalogs,
  isOptionDisabled,
  onCatalogCreateSuccess,
}: ArcticCatalogSelectProps) {
  const canAddCatalog = useSelector(
    (state: Record<string, any>) =>
      state.privileges.organization?.arcticCatalogs?.canCreate,
  );

  // Temporary configuration to filter out catalogs for ARS during project creation
  // const { filterCatalogs } = useContext(ArcticCatalogSelectConfig);

  const newArcticCatalog = useModalContainer();

  const handleArcticCatalogCreation = useCallback(
    (createdCatalog: ArcticCatalog) => {
      try {
        onCatalogCreateSuccess?.(createdCatalog);
        sessionStorage.setItem("newCatalogId", createdCatalog.id);
        onChange?.(createdCatalog.id, createdCatalog.name);
      } catch (e) {
        sentryUtil.logException(e);
      } finally {
        newArcticCatalog.close();
      }
    },
    [newArcticCatalog, onChange, onCatalogCreateSuccess],
  );

  const onSelectChange = useCallback(
    (value: string) => {
      if (disabled || pending) return;
      if (value === NEW_CATALOG_ITEM) {
        newArcticCatalog.open();
      } else if (catalogs) {
        const name = catalogs.find(
          (cur: ArcticCatalog) => cur.id === value,
        )?.name;
        onChange?.(value, name);
      }
    },
    [onChange, newArcticCatalog, catalogs, disabled, pending],
  );

  const options = useMemo(
    () => getOptions(catalogs, pending, isOptionDisabled, canAddCatalog),
    [catalogs, pending, isOptionDisabled, canAddCatalog],
  );

  return (
    <>
      <Select
        // searchable
        disabled={pending || disabled}
        placeholder={pending ? "" : placeholder}
        itemComponent={ArcticCatalogSelectItem}
        data={options}
        // maxDropdownHeight={200}
        value={value}
        onChange={onSelectChange}
        rightSection={<dremio-icon name="interface/caretDown" />}
        {...(pending && {
          icon: <Spinner />,
        })}
        error={error}
      />
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
  const handleChange = useCallback(
    (value: string | null, name?: string | null) => {
      field.onChange(value);
      fields.name.onChange(name);
    },
    [field, fields.name],
  );

  const { editing } = useContext(FormContext);
  const sources = useSelector(getSortedSources);
  const arcticSourceIds = useMemo(() => {
    return sources
      .toJS()
      .filter((cur: { type: string }) => cur.type === ARCTIC)
      .map((cur: any) => cur.config.arcticCatalogId);
  }, [sources]);
  const [catalogs, , status] = useArcticCatalogs();

  return (
    <FieldWithError
      {...field}
      label={intl.formatMessage({
        id: "VersionedSource.SelectCatalogFormItemLabel",
      })}
      name={elementConfig.getPropName()}
      className={fieldWithError}
      style={{
        width: "75%",
      }}
    >
      <ArcticCatalogSelectMemo
        catalogs={catalogs || []}
        isOptionDisabled={(catalog) => arcticSourceIds.includes(catalog.id)}
        disabled={editing || status === "pending"}
        pending={status === "pending"}
        value={field.value}
        onChange={handleChange}
        placeholder={intl.formatMessage({
          id: "VersionedSource.SelectCatalogPlaceholder",
        })}
      />
    </FieldWithError>
  );
}

export default ArcticCatalogSelectWrapper;
