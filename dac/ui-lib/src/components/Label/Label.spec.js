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

import Label from "./Label";

const defaultProps = {
  value: "Default Label",
  className: "label-test-class",
  styles: { color: "red" },
};

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<Label {...props} />);
};

describe("Label", () => {
  it("renders reqired components", () => {
    const wrapper = getShallowWrapper();
    expect(wrapper.find("div.label").exists()).toBe(true);
    expect(wrapper.find("div.label").text()).toEqual(defaultProps.value);
  });
});
