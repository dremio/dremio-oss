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
import React from "react";
import { shallow } from "enzyme";

import MuiSelect from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import { ReactComponent as ExpandMoreIcon } from "../../art/CaretDown.svg";

import Label from "../Label";

import Select from "./Select";

const mockOnChange = jest.fn();

const defaultProps = {
  label: "Sample label",
  name: "sample-radio-name",
  classes: {
    root: "sample-root-class",
    label: "sample-label-class",
  },
  onChange: mockOnChange,
  options: [
    { label: "label 1", value: "val1" },
    { label: "label 2", value: "val2" },
  ],
  value: "val1",
};

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<Select {...props} />);
};

describe("Radio", () => {
  const wrapper = getShallowWrapper();
  it("has the required components with props", () => {
    expect(wrapper.find("div.selectRoot").exists()).toBe(true);
    expect(wrapper.find(Label).exists()).toBe(true);
    expect(wrapper.find(Label).props().value).toEqual(defaultProps.label);
    expect(wrapper.find(MuiSelect).exists()).toBe(true);
    expect(wrapper.find(MuiSelect).props().IconComponent).toEqual(
      ExpandMoreIcon,
    );
    expect(wrapper.find(MuiSelect).props().value).toEqual(defaultProps.value);
    expect(wrapper.find(MenuItem).at(0).text()).toEqual(
      defaultProps.options[0].label,
    );
    expect(wrapper.find(MenuItem).at(0).props().value).toEqual(
      defaultProps.options[0].value,
    );
  });

  it("adds the classes passed as props to respective elements", () => {
    expect(wrapper.find("div.selectRoot").props().className).toEqual(
      expect.stringContaining("sample-root-class"),
    );
    expect(wrapper.find(Label).props().className).toEqual(
      expect.stringContaining("sample-label-class"),
    );
  });

  it("triggers onChange passed as prop", () => {
    const mockEvent = { key: "sample val" };
    wrapper.find(MuiSelect).at(0).simulate("change", mockEvent);
    expect(mockOnChange).toBeCalledTimes(1);
    expect(mockOnChange).toBeCalledWith(mockEvent);
  });
});
