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
import { PureComponent } from "react";
import Immutable from "immutable";
import clsx from "clsx";

import PropTypes from "prop-types";

import Spinner from "components/Spinner";
import DragSource from "components/DragComponents/DragSource";
import DatasetItemLabel from "components/Dataset/DatasetItemLabel";
import { constructFullPath } from "utils/pathUtils";
import { bodySmall } from "uiTheme/radium/typography";
import { getIconDataTypeFromDatasetType } from "utils/iconUtils";
import * as classes from "@app/components/DatasetList/DatasetList.module.less";

class DatasetList extends PureComponent {
  static propTypes = {
    data: PropTypes.instanceOf(Immutable.List).isRequired,
    showColumnLevel: PropTypes.bool,
    changeSelectedNode: PropTypes.func.isRequired,
    isInProgress: PropTypes.bool.isRequired,
    inputValue: PropTypes.string,
    dragType: PropTypes.string,
    showParents: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func,
    starNode: PropTypes.func,
    unstarNode: PropTypes.func,
    isStarredLimitReached: PropTypes.bool,
    starredItems: PropTypes.array,
    isExpandable: PropTypes.bool,
  };

  constructor(props) {
    super(props);
    this.state = {
      activeDataset: "",
    };
  }

  UNSAFE_componentWillReceiveProps(newProps) {
    if (newProps.data !== this.props.data) {
      this.resetSelectedData();
    }
  }

  setActiveDataset(node) {
    const fullPath = constructFullPath(node.get("fullPath"));
    this.setState({ activeDataset: fullPath });
    this.props.changeSelectedNode(fullPath, node);
  }

  getDatasetsList(data, inputValue) {
    const {
      shouldAllowAdd,
      addtoEditor,
      starNode,
      unstarNode,
      dragType,
      isStarredLimitReached,
      starredItems,
      isExpandable,
    } = this.props;
    return (
      data &&
      data.map &&
      data.map((value, key) => {
        const name = value.get("fullPath").get(value.get("fullPath").size - 1);
        const nodeId = value.get("id");
        const displayFullPath =
          value.get("displayFullPath") || value.get("fullPath");
        return (
          <DragSource
            dragType={dragType || ""}
            key={key}
            id={displayFullPath}
            className={classes["datasetItem"]}
          >
            <div
              key={key}
              style={bodySmall}
              className={"datasets-list"}
              onClick={this.setActiveDataset.bind(this, value)}
            >
              <DatasetItemLabel
                dragType={dragType}
                name={name}
                showFullPath
                inputValue={inputValue}
                fullPath={value.get("fullPath")}
                typeIcon={getIconDataTypeFromDatasetType(
                  value.get("datasetType"),
                )}
                placement="right"
                isExpandable={isExpandable}
                shouldShowOverlay
                shouldAllowAdd={shouldAllowAdd}
                addtoEditor={addtoEditor}
                displayFullPath={displayFullPath}
                starNode={starNode}
                unstarNode={unstarNode}
                nodeId={nodeId}
                isStarredLimitReached={isStarredLimitReached}
                isStarred={starredItems}
              />
            </div>
          </DragSource>
        );
      })
    );
  }

  resetSelectedData() {
    this.setState({ activeDataset: "" });
    this.props.changeSelectedNode(null);
  }

  render() {
    const { data, inputValue, isInProgress } = this.props;
    const searchBlock =
      data && data.size && data.size > 0 ? (
        this.getDatasetsList(data, inputValue)
      ) : (
        <div className={classes["notFound"]}>
          {laDeprecated("No results found")}
        </div>
      );
    return (
      <div className={clsx(classes["dataSetsList"], "datasets-list")}>
        {isInProgress ? <Spinner /> : searchBlock}
      </div>
    );
  }
}

export default DatasetList;
