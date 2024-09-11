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

import * as React from "react";
import { forwardRef } from "react";
import { Popover } from "../Popover";
import { Checkbox } from "../Checkbox";
import { Input } from "../Input";
import { Button } from "../Button/Button";
import clsx from "clsx";

type Item = { label: string; id: string; icon?: string };

type MultiCheckboxPopoverProps = {
  listItems: Item[];
  listLabel: string;
  onItemSelect: (filterKey: string) => void;
  onItemUnselect: (filterKey: string) => void;
  selectedtListItems?: Item[];
  hasSearch?: boolean;
  searchPlaceholder?: string;
  onSearch?: (e: React.ChangeEvent<HTMLInputElement>) => void;
  className?: string;
  buttonProps?: {
    leftIcon?: JSX.Element;
    rightIcon?: JSX.Element;
    content?: string | JSX.Element;
  };
  ariaLabel?: string;
};

export const MultiCheckboxPopover = forwardRef<
  HTMLDivElement,
  MultiCheckboxPopoverProps
>((props, ref) => {
  const { selectedtListItems = [] } = props;
  const [search, setSearch] = React.useState("");

  const handleCheck = (newItem: Item) => {
    if (!selectedtListItems.find((item) => item.id === newItem.id)) {
      props.onItemSelect(newItem.id);
    } else {
      props.onItemUnselect(newItem.id);
    }
  };

  const onSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (props.onSearch) {
      props.onSearch(e);
    } else {
      setSearch(e.target.value);
    }
  };

  const filteredListItems = props.onSearch
    ? props.listItems
    : props.listItems.filter((item) =>
        item.label.toLowerCase().includes(search.toLowerCase()),
      );

  const valuesText =
    selectedtListItems.length === 0
      ? ""
      : selectedtListItems.length === 1
        ? selectedtListItems[0].label
        : `${selectedtListItems[0].label}, +${selectedtListItems.length - 1}`;

  return (
    <div className={clsx("multi-checkbox-popover", props.className)} ref={ref}>
      <Popover
        mode="click"
        role="listbox"
        placement="bottom-start"
        dismissable
        portal={false}
        content={
          <>
            <ul className="listbox">
              <li className="listbox-group-label text-md text-semibold">
                {props.listLabel}
              </li>
              {props.hasSearch && (
                <div className="pl-1 pr-1">
                  <Input
                    placeholder={props.searchPlaceholder || "Search"}
                    onChange={onSearch}
                    className="mt-1 mb-1"
                  />
                </div>
              )}
              <ul className="listbox-group">
                {filteredListItems.map((item) => (
                  <li key={item.id} className="listbox-item">
                    <Checkbox
                      label={
                        <>
                          {item?.icon && (
                            // @ts-ignore
                            <dremio-icon
                              style={{ width: 24, height: 24 }}
                              name={item.icon}
                            />
                          )}
                          {item.label}
                        </>
                      }
                      style={{ minWidth: 16 }}
                      className="w-full flex items-center gap-05"
                      value={item.id}
                      onClick={() => handleCheck(item)}
                      checked={
                        !!selectedtListItems.find(
                          (selItem) => selItem.id === item.id,
                        )
                      }
                    />
                  </li>
                ))}
              </ul>
            </ul>
          </>
        }
      >
        <Button
          variant="secondary"
          className={clsx(
            "multi-checkbox-popover__button",
            valuesText && "multi-checkbox-popover__button__hasValues",
          )}
        >
          <div
            className="w-full flex justify-between items-center flex-1"
            aria-label={`${props.ariaLabel}${valuesText.length ? `, ${valuesText}` : ""}`}
          >
            {props?.buttonProps?.leftIcon || null}
            <span className="truncate">
              {props?.buttonProps?.content || valuesText || props.listLabel}
            </span>
            {props?.buttonProps?.rightIcon || (
              // @ts-ignore
              <dremio-icon name="interface/caretDown" />
            )}
          </div>
        </Button>
      </Popover>
    </div>
  );
});
