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
import { TreeNode } from "./TreeNode";
import { starTabNames } from "#oss/components/Tree/resourceTreeUtils";

describe("TreeNode", () => {
  let commonProps;
  beforeEach(() => {
    commonProps = {
      node: Immutable.fromJS({
        id: "123",
        type: "VDS",
        fullPath: Immutable.fromJS(["node"]),
        resources: [
          { fullPath: Immutable.fromJS(["node", "resource1"]) },
          { fullPath: Immutable.fromJS(["node", "resource2"]) },
          { fullPath: Immutable.fromJS(["node", "resource3"]) },
        ],
      }),
      isNodeExpanded: sinon.stub(),
      selectedNodeId: "123",
      isDatasetsDisabled: true,
      shouldAllowAdd: true,
      shouldShowOverlay: false,
      addtoEditor: sinon.stub(),
      handleSelectedNodeChange: sinon.stub(),
      formatIdFromNode: sinon.stub(),
      isNodeExpandable: sinon.stub(),
      isExpandable: true,
      starredItems: [],
      starNode: sinon.stub().returns({
        then: (callback) => callback({}),
      }),
      unstarNode: sinon.stub().returns({
        then: (callback) => callback({}),
      }),
      tabRendered: starTabNames.all,
      expandedNodes: Immutable.List(),
      expandedStarredNodes: Immutable.List(),
      isStarredLimitReached: false,
      hideDatasets: false,
      hideSpaces: false,
      hideSources: false,
      hideHomes: false,
      loadingItems: Immutable.Map(
        {
          123: [
            {
              error: {
                message: {
                  errorMessage:
                    "Cannot provide more information about this dataset.",
                },
                id: "321",
                dismissed: false,
              },
              isFailed: true,
              isInProgress: false,
            },
          ],
        },
        {
          "resource1-1": [
            {
              error: null,
              isFailed: false,
              isInProgress: false,
            },
          ],
        },
      ),
    };
  });

  describe("TreeNode rendering", () => {
    it("should render child TreeNode for each node in props.node.resources if isNodeExpanded returns true", () => {
      commonProps.isNodeExpanded.returns(true);
      const wrapper = shallow(<TreeNode {...commonProps} />);
      expect(wrapper.find("Memo(TreeNode)")).to.have.length(3);
    });

    it("should pass props to child TreeNode", () => {
      commonProps.isNodeExpanded.returns(true);
      const wrapper = shallow(<TreeNode {...commonProps} />);
      const childNodeProps = wrapper.find("Memo(TreeNode)").first().props();
      expect(childNodeProps.isNodeExpanded).to.equal(
        commonProps.isNodeExpanded,
      );
      expect(childNodeProps.selectedNodeId).to.equal(
        commonProps.selectedNodeId,
      );
    });

    it("should not any TreeNode components if isNodeExpanded returns false", () => {
      commonProps.isNodeExpanded.returns(false);

      const wrapper = shallow(<TreeNode {...commonProps} />);
      expect(wrapper.find("Memo(TreeNode)")).to.have.length(0);
    });
  });

  describe("handleLoadingState", () => {
    beforeEach(() => {
      commonProps.isNodeExpandable.returns(true);
    });
    it("show expandable node with arrow and no statuses", () => {
      const wrapper = shallow(<TreeNode {...commonProps} />);
      expect(
        wrapper.find(".TreeNode").first().hasClass("TreeNode__arrowIcon"),
      ).to.equal(true);
    });
    // Fix test so it works with a setTimeout
    xit("show loading spinner for node", () => {
      const wrapper = shallow(<TreeNode {...commonProps} />);

      wrapper.setProps({ loadingItems: { resource1: { inProgress: true } } });

      setTimeout(() => {
        expect(wrapper.find("Spinner")).to.have.length(1);
      }, 200);
    });
  });
});
