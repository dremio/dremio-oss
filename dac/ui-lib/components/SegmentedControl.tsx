/* eslint-disable react/prop-types */
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

/* eslint-disable react/prop-types */
import * as React from "react";
import { forwardRef, useState, useCallback } from "react";
import clsx from "clsx";

type SegmentedControlProps<T extends string = string> = {
  children: JSX.Element[];
  onChange: (value: T) => void;
  value: T;
} & React.DetailedHTMLProps<
  React.HTMLAttributes<HTMLDivElement>,
  HTMLDivElement
>;

/**
 * Allows a user to make a single selection from a set of 2–5 options
 */
export const SegmentedControl = forwardRef(
  (
    props: SegmentedControlProps,
    ref: React.ForwardedRef<HTMLDivElement>
  ): JSX.Element => {
    const { className, children, onChange, value, ...rest } = props;
    const handleOptionSelection = useCallback(
      (newValue: string) => {
        if (newValue !== value) {
          onChange(newValue);
        }
      },
      [onChange, value]
    );
    return (
      <div
        {...rest}
        ref={ref}
        className={clsx("dremio-segmented-control", className)}
      >
        {React.Children.map(children, (child) => {
          return React.cloneElement(child, {
            onSelected: handleOptionSelection,
            selected: value === child.props.value,
          });
        })}
      </div>
    );
  }
);

SegmentedControl.displayName = "SegmentedControl";

type SegmentedControlOptionProps<T extends string = string> = {
  onSelected: (value: T) => void;
  selected: boolean;
  value: T;
} & React.DetailedHTMLProps<
  React.HTMLAttributes<HTMLButtonElement>,
  HTMLButtonElement
>;

export const SegmentedControlOption = forwardRef(
  (
    props: SegmentedControlOptionProps,
    ref: React.ForwardedRef<HTMLButtonElement>
  ): JSX.Element => {
    const {
      className,
      children,
      selected,
      value,
      onSelected,
      onClick,
      ...rest
    } = props;
    return (
      <button
        {...rest}
        ref={ref}
        className={clsx("dremio-segmented-control-option", className)}
        role="button"
        aria-pressed={selected ? "true" : "false"}
        onClick={(e) => {
          e.stopPropagation();
          onClick?.(e);
          onSelected(value);
        }}
      >
        {children}
      </button>
    );
  }
);

SegmentedControlOption.displayName = "SegmentedControlOption";

/**
 * Maintains state for a SegmentedControl and provides required props to it
 */
export const useSegmentedControl = <T extends string>(initialState: T) => {
  const [value, setValue] = useState(initialState);
  return {
    onChange: useCallback((newValue: T) => {
      setValue(newValue);
    }, []),
    value,
  };
};
