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
import { DatasetItemLabel } from "./DatasetItemLabel"; // {} for testing purposes to avoid redux store issues

describe("DatasetItemLabel", () => {
  let commonProps;
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      fullPath: Immutable.List(["Prod-sample", "ds1"]),
      typeIcon: "VirtualDataset",
      intl: {
        formatMessage: sinon.spy(),
      },
      showFullPath: false,
      isOpenOverlay: false,
      inputValue: "Fake Value",
      isNewQuery: false,
      placement: "top",
      shouldShowOverlay: false,
      shouldAllowAdd: false,
      addtoEditor: sinon.spy(),
      isExpandable: false,
      isStarred: false,
      nodeId: "fake-id-for-this-node",
      setShowOverlay: sinon.spy(),
      starNode: sinon.spy(),
      unstarNode: sinon.spy(),
      isStarredLimitReached: false,
    };
    commonProps = {
      ...minimalProps,
      showFullPath: false,
      isOpenOverlay: false,
      inputValue: "Fake Value",
      isNewQuery: false,
      placement: "top",
      shouldShowOverlay: false,
      shouldAllowAdd: false,
      addtoEditor: sinon.spy(),
      isExpandable: false,
      isStarred: false,
      nodeId: "fake-id-for-this-node",
      setShowOverlay: sinon.spy(),
      starNode: sinon.spy(),
      unstarNode: sinon.spy(),
      isStarredLimitReached: false,
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<DatasetItemLabel {...minimalProps} />);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find("EllipsedText").first().props().text).to.equal("ds1");
  });

  it("should render EllipsedText", () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps} />);
    expect(wrapper.find("EllipsedText")).to.have.length(1);
  });

  it("should show fullPath in header", () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps} />);
    expect(wrapper.find("EllipsedText")).to.have.length(1);
    wrapper.setProps({ showFullPath: true });
    expect(wrapper.find("EllipsedText")).to.have.length(2);
  });

  it("should render custom node", () => {
    const customNode = <div className="customNode">DG10</div>;
    const wrapper = shallow(
      <DatasetItemLabel {...commonProps} customNode={customNode} />,
    );
    expect(wrapper.find(".customNode")).to.have.length(1);
    expect(wrapper.find("EllipsedText")).to.have.length(0);
  });
});
