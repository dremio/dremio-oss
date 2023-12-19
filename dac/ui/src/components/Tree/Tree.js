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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";

import TreeNode from "./TreeNode";

class Tree extends Component {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    isNodeExpanded: PropTypes.func,
    selectedNodeId: PropTypes.string,
    formatIdFromNode: PropTypes.func,
    isDatasetsDisabled: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    dragType: PropTypes.any,
    shouldShowOverlay: PropTypes.bool,
    addtoEditor: PropTypes.func,
    handleSelectedNodeChange: PropTypes.func,
    isNodeExpandable: PropTypes.func,
    isExpandable: PropTypes.bool,
    fromModal: PropTypes.bool,
    starredItems: PropTypes.array,
    starNode: PropTypes.func,
    unstarNode: PropTypes.func,
  };

  static defaultProps = {
    resourceTree: Immutable.List(),
    fromModal: false,
  };

  renderNodes = () => {
    const { resourceTree, starredItems } = this.props;
    return resourceTree.map((node, index) => (
      <TreeNode
        node={node}
        key={index}
        isStarredLimitReached={starredItems && starredItems.length === 25}
        {...this.props}
      />
    ));
  };

  render() {
    return (
      <div style={style} className="global-tree">
        {this.renderNodes()}
      </div>
    );
  }
}

const style = {
  width: "100%",
};

export default Tree;
