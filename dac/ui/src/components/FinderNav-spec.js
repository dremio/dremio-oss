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
import { shallow } from "enzyme";

import Immutable from "immutable";

import { FinderNav } from "./FinderNav";
import FinderNavSection from "./FinderNavSection";

describe("FinderNav", () => {
  let commonProps;
  beforeEach(() => {
    commonProps = {
      title: "Sources",
      addToolTip: "Add Sources",
      navItems: new Immutable.List([]),
      isInProgress: false,
      listHref: "#Hello",
    };
  });

  it("shows the nav item list section", () => {
    const wrapper = shallow(<FinderNav {...commonProps} />);
    expect(wrapper.find(FinderNavSection)).to.have.length(1);
  });

  it("does not show the nav item list seciton when it is loading", () => {
    const wrapper = shallow(<FinderNav {...commonProps} isInProgress />);
    expect(wrapper.find(FinderNavSection)).to.have.length(0);
  });

  it("shows the toggle control if it is collapsiable", () => {
    // Collapsible case
    let wrapper = shallow(<FinderNav {...commonProps} isCollapsible />);
    expect(wrapper.find(".icon-container")).to.have.length(1);
    // Not-collapsible case
    wrapper = shallow(<FinderNav {...commonProps} isCollapsible={false} />);
    expect(wrapper.find(".icon-container")).to.have.length(0);
  });

  it("shows the nav item section if it is collapsiable and is expanded", () => {
    // Collapsed case
    let wrapper = shallow(
      <FinderNav {...commonProps} isCollapsible isCollapsed />
    );
    expect(wrapper.hasClass("finder-nav--collapsed")).to.be.true;
    // Expanded case
    wrapper = shallow(
      <FinderNav {...commonProps} isCollapsible isCollapsed={false} />
    );
    expect(wrapper.hasClass("finder-nav--collapsed")).to.be.false;
  });

  it("invoke the toggle callback when the toggle control is clicked", () => {
    const onToggle = sinon.spy();
    const instance = shallow(
      <FinderNav {...commonProps} isCollapsible onToggle={onToggle} />
    ).instance();
    instance.onToggleClick();
    expect(onToggle).to.be.calledOnce;
  });
});
