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
//@ts-nocheck
import * as React from "react";
import {
  useCallback,
  useState,
  type DetailedHTMLProps,
  type OlHTMLAttributes,
  type ReactNode,
} from "react";
import { useSelect as useDownshiftSelect } from "downshift";
import { FloatingContainer } from "../FloatingContainer";
import { SelectRegisterValueContext } from "./SelectRegisterValueContext";
import { SelectOptionContext } from "./SelectOptionContext";

type SelectProps = {
  children: ReactNode;
  disabled?: boolean;
  onChange: (value: string) => void;
  onClosed?: () => void;
  onOpened?: () => void;
  renderButtonLabel: (option: string | null) => ReactNode;
  value: string | null;
} & DetailedHTMLProps<OlHTMLAttributes<HTMLUListElement>, HTMLUListElement>;

export const Select = (props: SelectProps) => {
  const [values, setValues] = useState([]);
  const registerValue = useCallback((value) => {
    setValues((x) => [...x, value]);
  }, []);
  const downshiftSelect = useDownshiftSelect({
    items: values,
    selectedItem: props.value || null,
    onSelectedItemChange: ({ selectedItem }) => {
      props.onChange?.(selectedItem || null);
    },
    onIsOpenChange: ({ isOpen }) => {
      if (isOpen) {
        props.onOpened?.();
      } else {
        props.onClosed?.();
      }
    },
  });
  const {
    isOpen,
    selectedItem,
    getToggleButtonProps,
    getMenuProps,
    getItemProps,
    highlightedIndex,
  } = downshiftSelect;
  return (
    <FloatingContainer
      isOpen={isOpen}
      trigger={
        <button
          className="form-control no-select"
          disabled={props.disabled}
          type="button"
          {...getToggleButtonProps()}
          style={props.style}
        >
          {props.renderButtonLabel(selectedItem)}
          {/*@ts-ignore*/}
          <dremio-icon name="interface/select-expand" class="ml-auto" alt="" />
        </button>
      }
    >
      <ul className="listbox float-container no-select" {...getMenuProps()}>
        <SelectOptionContext.Provider
          value={{
            getOptionProps: ({ value, disabled }) => {
              const valueIndex = values.indexOf(value);
              if (valueIndex === -1) {
                return null;
              }
              return getItemProps({ index: valueIndex, item: value, disabled });
            },
            isHighlighted: (value) => {
              const valueIndex = values.indexOf(value);
              return valueIndex !== -1 && highlightedIndex === valueIndex;
            },
          }}
        >
          <SelectRegisterValueContext.Provider value={registerValue}>
            {props.children}
          </SelectRegisterValueContext.Provider>
        </SelectOptionContext.Provider>
      </ul>
    </FloatingContainer>
  );
};
