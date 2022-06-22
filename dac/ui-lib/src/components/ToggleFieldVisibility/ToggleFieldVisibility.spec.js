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
import SvgIcon from "@material-ui/core/SvgIcon";

import ToggleFieldVisibility from "./ToggleFieldVisibility";

const defaultProps = {};

const getShallowWrapper = (props = {}) => {
  const compProps = {
    ...defaultProps,
    ...props,
  };
  return shallow(<ToggleFieldVisibility {...compProps} />);
};

describe("ToggleFieldVisibility", () => {
  it("renders reqired components", () => {
    const onView = jest.fn();
    const wrapper = getShallowWrapper();
    const element = wrapper.find("div");
    expect(element.exists()).toBe(true);
    expect(element.find(SvgIcon).exists()).toBe(true);
    element.simulate("click");
    expect(onView).not.toHaveBeenCalled();
    const updatedWrapper = getShallowWrapper({ onView });
    const updatedElement = updatedWrapper.find("div");
    expect(updatedElement.exists()).toBe(true);
    updatedElement.simulate("click");
    expect(onView).toHaveBeenCalled();
  });
});
