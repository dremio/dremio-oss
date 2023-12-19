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
import { type ReactNode, useContext, useEffect } from "react";
import { SelectRegisterValueContext } from "./SelectRegisterValueContext";
import { SelectOptionContext } from "./SelectOptionContext";

type SelectOptionProps = {
  disabled?: boolean;
  children: ReactNode;
  value: string;
};

export const SelectOption = (props: SelectOptionProps) => {
  const registerValue = useContext(SelectRegisterValueContext);
  const { getOptionProps, isHighlighted } = useContext(SelectOptionContext);
  const { children, value, ...rest } = props;
  useEffect(() => {
    registerValue(value);
  }, [registerValue, value]);
  return (
    <li
      {...rest}
      className={`${
        isHighlighted(value) ? " listbox-item--active" : ""
      } listbox-item`}
      {...getOptionProps({ value, disabled: props.disabled })}
    >
      {children}
    </li>
  );
};
