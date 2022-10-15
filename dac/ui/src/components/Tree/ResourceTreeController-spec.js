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
import ResourceTreeController from "./ResourceTreeController";

describe("ResourceTreeController", () => {
  let minimalProps, commonProps, wrapper, instance;
  beforeEach(() => {
    minimalProps = {
      updateTreeNodeData: sinon.stub().returns({
        then: (callback) => callback({}),
      }),
    };
    commonProps = {
      ...minimalProps,
      sidebarCollapsed: false,
      onChange: sinon.spy(),
      tabRendered: "All",
      isSqlEditorTab: true,
      dispatchSetActiveScript: sinon.spy(),
      resourceTree: Immutable.fromJS([
        {
          type: "SPACE",
          name: "DG",
          fullPath: ["DG"],
          resources: [
            {
              type: "FOLDER",
              name: "fold",
              fullPath: ["DG", "fold"],
              resources: [
                {
                  type: "VIRTUAL_DATASET",
                  name: "ds",
                  fullPath: ["DG", "fold", "ds"],
                },
              ],
            },
          ],
        },
        {
          type: "SOURCE",
          name: "dfs",
          fullPath: ["dfs"],
        },
        {
          type: "HOME",
          name: "@dremio",
          fullPath: ["@dremio"],
        },
      ]),
    };
    wrapper = shallow(<ResourceTreeController {...commonProps} />);
    instance = wrapper.instance();
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<ResourceTreeController {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  it("should render with common props without exploding", () => {
    expect(wrapper).to.have.length(1);
  });

  describe("componentDidMount", () => {
    const ININTAL_LOAD = true;
    it("should initilize the resource tree", async () => {
      const props = {
        ...minimalProps,
        resourceTree: Immutable.fromJS([]),
      };
      const wrapper = shallow(<ResourceTreeController {...props} />);
      const instance = wrapper.instance();
      sinon.spy(instance, "initializeTree");
      await instance.componentDidMount();
      expect(wrapper.instance().initializeTree.called).to.be.true;
      expect(wrapper.instance().initializeTree.calledWith(ININTAL_LOAD)).to.be
        .true;
    });
  });

  describe("componentDidUpdate", () => {
    it("should initilize the resource tree because treeInitialized has not been initilized", async () => {
      const prevProps = {
        sidebarCollapsed: true,
        isSqlEditorTab: true,
      };
      instance.setState({ treeInitialized: false });
      sinon.spy(instance, "initializeTree");
      instance.componentDidUpdate(prevProps);
      expect(instance.initializeTree.called).to.be.true;
    });
    it("should initilize the resource tree because resourceTree is empty", async () => {
      const prevProps = {
        sidebarCollapsed: false,
      };
      const props = {
        ...minimalProps,
        sidebarCollapsed: true,
        resourceTree: Immutable.fromJS([]),
      };
      const wrapper = shallow(<ResourceTreeController {...props} />);
      const instance = wrapper.instance();
      instance.setState({ treeInitialized: false });
      sinon.spy(instance, "initializeTree");
      instance.componentDidUpdate(prevProps);
      expect(instance.initializeTree.called).to.be.true;
    });
    it("dispatchSetActiveScript", async () => {
      const prevProps = {
        isSqlEditorTab: true,
      };
      const props = {
        ...commonProps,
        isSqlEditorTab: false,
      };
      const wrapper = shallow(<ResourceTreeController {...props} />);
      const instance = wrapper.instance();
      instance.componentDidUpdate(prevProps);
      expect(commonProps.dispatchSetActiveScript).to.have.been.called;
    });
  });

  describe("handleSelectedNodeChange", () => {
    it("should call onChange with selectedNodeId and node and set state with selectedNodeId", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const node = {
        type: "SPACE",
        name: "DG",
        fullPath: ["DG"],
      };
      instance.handleSelectedNodeChange(node.name, node);
      expect(commonProps.onChange.called).to.be.true;
      expect(commonProps.onChange.calledWith(node.name, node)).to.be.true;
      expect(wrapper.state("selectedNodeId")).to.equal(node.name);
    });
  });
  describe("isNodeExpanded", () => {
    it("should return true if node is expanded", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const node = Immutable.fromJS({
        type: "SPACE",
        name: "DG",
        fullPath: ["DG"],
      });
      const expandedNodes = Immutable.fromJS([node.get("name")]);

      wrapper.setState({ expandedNodes });
      expect(instance.isNodeExpanded(node)).to.be.true;
    });
    it("should return false if node is not expanded", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const node = Immutable.fromJS({
        type: "SPACE",
        name: "DG",
        fullPath: ["DG"],
      });
      expect(instance.isNodeExpanded(node)).to.be.false;
    });
  });
  describe("expandPathToSelectedNode", () => {
    it("should add all parent combinations as dotted paths to state.expandedNodes", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const path = "DG.folder.asd";
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state("expandedNodes")).to.be.equal(
        Immutable.List(["DG", "DG.folder"])
      );
    });

    it("should add nothing to state.expandedNodes if no parents exist", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const path = "asd";
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state("expandedNodes").size).to.be.equal(0);
    });

    it("should correctly handle paths with doublequotes", () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps} />);
      const instance = wrapper.instance();
      const path = '"@drem.io".folder.another.andanother';
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state("expandedNodes")).to.be.equal(
        Immutable.List([
          '"@drem.io"',
          '"@drem.io".folder',
          '"@drem.io".folder.another',
        ])
      );
    });
  });
  describe("handleNodeClick", () => {
    let wrapper;
    let instance;
    let node;
    beforeEach(() => {
      wrapper = shallow(<ResourceTreeController {...commonProps} />);
      instance = wrapper.instance();
      node = commonProps.resourceTree.get(0);
    });
    it("should set selectedNodeId, if node does not exists in expandedNodes then add it, call loadResourceTree", () => {
      instance.handleNodeClick(node);
      expect(commonProps.updateTreeNodeData.called).to.be.true;
      expect(commonProps.updateTreeNodeData.calledWith(false, node.get("name")))
        .to.be.true;
      expect(wrapper.state("selectedNodeId")).to.eql(node.get("name"));
      expect(wrapper.state("expandedNodes").includes(node.get("name"))).to.be
        .true;
    });
    it("should delete node from expandedNodes if it exists, with children", () => {
      wrapper.setState({ expandedNodes: Immutable.fromJS(["DG", "DG.fold"]) });
      instance.handleNodeClick(node);
      expect(commonProps.updateTreeNodeData.called).to.be.true;
      expect(commonProps.updateTreeNodeData.calledWith(false, node.get("name")))
        .to.be.true;
      expect(wrapper.state("expandedNodes").size).to.be.equal(0);
    });
  });
});
