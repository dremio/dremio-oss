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

import FormControl from "@material-ui/core/FormControl";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import RadioGroup from "@material-ui/core/RadioGroup";
import MaterialRadio from "@material-ui/core/Radio";

import Label from "../Label";

import Radio from "./Radio";

const mockOnChange = jest.fn();

const defaultProps = {
  label: "Sample label",
  name: "sample-radio-name",
  classes: {
    root: "sample-root-class",
    label: "sample-label-class",
    optionsContainer: "sample-options-container-class",
  },
  onChange: mockOnChange,
  options: [
    { label: "label 1", value: "val1" },
    { label: "label 2", value: "val2" },
  ],
};

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<Radio {...props} />);
};

describe("Radio", () => {
  const wrapper = getShallowWrapper();
  it("has the required components with props", () => {
    expect(wrapper.find("div.radio-root").exists()).toBe(true);
    expect(wrapper.find(Label).exists()).toBe(true);
    expect(wrapper.find(Label).props().value).toEqual(defaultProps.label);
    expect(wrapper.find(FormControl).exists()).toBe(true);
    expect(wrapper.find(RadioGroup).exists()).toBe(true);
    expect(wrapper.find(FormControlLabel).length).toBe(
      defaultProps.options.length
    );
    expect(wrapper.find(FormControlLabel).at(0).props()).toEqual(
      expect.objectContaining({
        control: <MaterialRadio color="primary" />,
        label: defaultProps.options[0].label,
        value: defaultProps.options[0].value,
      })
    );
  });

  it("adds the classes passed as props to respective elements", () => {
    expect(wrapper.find("div.radio-root").props().className).toEqual(
      expect.stringContaining("sample-root-class")
    );
    expect(wrapper.find(Label).props().className).toEqual(
      expect.stringContaining("sample-label-class")
    );
    expect(wrapper.find(FormControl).props().classes.root).toEqual(
      expect.stringContaining("sample-options-container-class")
    );
  });

  it("aligns according to prop", () => {
    expect(wrapper.find(RadioGroup).props().row).toBe(false);
    const rowAlignWrapper = getShallowWrapper({
      ...defaultProps,
      align: "row",
    });
    expect(rowAlignWrapper.find(RadioGroup).props().row).toBe(true);
  });

  it("triggers onChange passed as prop", () => {
    const mockEvent = { key: "sample val" };
    wrapper.find(RadioGroup).at(0).simulate("change", mockEvent);
    expect(mockOnChange).toBeCalledTimes(1);
    expect(mockOnChange).toBeCalledWith(mockEvent);
  });
});
