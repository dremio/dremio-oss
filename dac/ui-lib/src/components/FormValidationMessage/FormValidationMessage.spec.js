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

import FormValidationMessage from "./FormValidationMessage";

const getShallowWrapper = (children, props = {}) => {
  return shallow(
    <FormValidationMessage {...props}>{children}</FormValidationMessage>,
  );
};

describe("Form Validation Message", () => {
  const wrapper = getShallowWrapper("Message");

  it("renders required components", () => {
    expect(wrapper.find("div.validationError").exists()).toBe(true);
    expect(wrapper.find("div").length).toEqual(2);
  });

  it("handles single message", () => {
    expect(wrapper.find("div.validationError").exists()).toBe(true);
    expect(wrapper.find("div").length).toEqual(2);
    expect(wrapper.find("div").at(1).text()).toEqual("Message");
  });

  it("handles multiple messages", () => {
    const multipleMesssageWrapper = getShallowWrapper([
      "Message 1",
      "Message 2",
    ]);
    expect(multipleMesssageWrapper.find("div.validationError").exists()).toBe(
      true,
    );
    expect(multipleMesssageWrapper.find("div").length).toEqual(3);
    expect(multipleMesssageWrapper.find("div").at(1).text()).toEqual(
      "Message 1",
    );
    expect(multipleMesssageWrapper.find("div").at(2).text()).toEqual(
      "Message 2",
    );
  });
});
