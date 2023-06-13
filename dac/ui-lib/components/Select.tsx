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
import React from "react";
import { useSelect as useDownshiftSelect } from "downshift";
import { useState } from "react";
import { FloatingContainer } from "./FloatingContainer";
import { type ReactNode } from "react";

type SelectProps<T extends readonly any[]> = {
  onChange: (value: T) => void;
  options: T;
  renderOptionLabel: (value: T[number]) => ReactNode;
  renderButtonLabel: (value: T[number] | null) => ReactNode;
  value: T[number] | null;
  disabled?: boolean;
  onOpened?: () => void;
  onClosed?: () => void;
  renderLoading?: () => ReactNode;
};

export const useSelect = <T extends readonly any[]>(
  initial: T | null = null
) => {
  const [selected, setSelected] = useState<T | null>(initial);

  return {
    value: selected,
    onChange: (value: T) => {
      setSelected(value);
    },
  };
};

export const Select = <T extends readonly any[]>(props: SelectProps<T>) => {
  const {
    isOpen,
    selectedItem,
    getToggleButtonProps,
    getMenuProps,
    highlightedIndex,
    getItemProps,
  } = useDownshiftSelect({
    items: props.options || [],
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
  return (
    <FloatingContainer
      isOpen={isOpen}
      trigger={
        <button
          className="form-control no-select"
          disabled={props.disabled}
          type="button"
          {...getToggleButtonProps()}
        >
          {props.renderButtonLabel(selectedItem)}
          <dremio-icon
            name="interface/caretDown"
            class="ml-auto"
            alt=""
          ></dremio-icon>
        </button>
      }
    >
      <ul className="listbox float-container no-select" {...getMenuProps()}>
        {!props.options && props.renderLoading()}
        {!!props.options &&
          props.options.map((item, index) => (
            <li
              className={`${
                highlightedIndex === index ? " listbox-item--active" : ""
              }`}
              key={`${item}${index}`}
              {...getItemProps({
                item,
                index,
              })}
            >
              {props.renderOptionLabel(item)}
            </li>
          ))}
      </ul>
    </FloatingContainer>
  );
};
