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

import ExternalLink from "./ExternalLink";

const mockOnClick = jest.fn();

const defaultProps = {
  href: "exampledomain.com",
  onClick: mockOnClick,
  className: "sample-class",
};

const defaultChildren = <span id="child"> Child </span>;

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<ExternalLink {...props}>{defaultChildren}</ExternalLink>);
};

describe("External Link", () => {
  const wrapper = getShallowWrapper();
  it("displays the required components", () => {
    expect(wrapper.find("a").exists()).toBe(true);
    expect(wrapper.find("#child").exists()).toBe(true);
    expect(wrapper.find("a").props()).toEqual(
      expect.objectContaining({
        href: defaultProps.href,
        onClick: mockOnClick,
        target: "_blank",
        rel: "noopener noreferrer",
        className: "sample-class",
      })
    );
  });

  it("triggers onClick sent in props", () => {
    wrapper.find("a").at(0).simulate("click");
    expect(mockOnClick).toBeCalledTimes(1);
  });
});
