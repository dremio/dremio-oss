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

import { ReactComponent as Expand } from "../../art/ArrowRight.svg";
import { ReactComponent as Collapse } from "../../art/ArrowDown.svg";

import ExpandableText from "./ExpandableText";

const defaultProps = {
  label: "Sample Label",
  classes: {
    root: "sample-root-class",
    labelContainer: "sample-label-container-class",
    label: "sample-label-class",
  },
};

const defaultChildren = <span id="children">Children</span>;

const getShallowWrapper = (
  props = defaultProps,
  children = defaultChildren,
) => {
  return shallow(<ExpandableText {...props}>{children}</ExpandableText>);
};

describe("Expandable Text", () => {
  it("has the required components and collapses text by default", () => {
    const wrapper = getShallowWrapper();
    expect(wrapper.find("ExpandableTextRoot").exists()).toBe(true);
    expect(wrapper.find(".expandable-text-label-container").exists()).toBe(
      true,
    );
    expect(wrapper.find(".expandable-text-label").exists()).toBe(true);
    expect(wrapper.find(".collapsable-container").exists()).toBe(false);
    expect(wrapper.find("#children").exists()).toBe(false);
  });

  it("adds the classes passed as props to respective elements", () => {
    const wrapper = getShallowWrapper();
    expect(wrapper.find("ExpandableTextRoot").props().className).toEqual(
      expect.stringContaining("sample-root-class"),
    );
    expect(
      wrapper.find(".expandable-text-label-container").props().className,
    ).toEqual(expect.stringContaining("sample-label-container-class"));
    expect(wrapper.find(".expandable-text-label").props().className).toEqual(
      expect.stringContaining("sample-label-class"),
    );
  });

  it("toggles visibility of collable content on click of label", () => {
    const wrapper = getShallowWrapper();
    wrapper.find(".expandable-text-label-container").at(0).simulate("click");
    expect(wrapper.find(".collapsable-container").exists()).toBe(true);
    expect(wrapper.find("#children").exists()).toBe(true);
    wrapper.find(".expandable-text-label-container").at(0).simulate("click");
    expect(wrapper.find(".collapsable-container").exists()).toBe(false);
    expect(wrapper.find("#children").exists()).toBe(false);
  });

  it("toggles expand/hide icon as applicable", () => {
    const wrapper = getShallowWrapper();
    wrapper.find(".expandable-text-label-container").at(0).simulate("click");
    expect(wrapper.find(Expand).exists()).toBe(false);
    expect(wrapper.find(Collapse).exists()).toBe(true);
    wrapper.find(".expandable-text-label-container").at(0).simulate("click");
    expect(wrapper.find(Expand).exists()).toBe(true);
    expect(wrapper.find(Collapse).exists()).toBe(false);
  });

  it("shows the content by default when defaultExpanded is true", () => {
    const wrapper = getShallowWrapper({
      ...defaultProps,
      defaultExpanded: true,
    });
    expect(wrapper.find(".collapsable-container").exists()).toBe(true);
    expect(wrapper.find("#children").exists()).toBe(true);
  });
});
