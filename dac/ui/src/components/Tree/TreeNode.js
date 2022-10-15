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
import { useEffect, useRef, useState } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import classNames from "classnames";
import { intl } from "@app/utils/intl";
import { DATASET_TYPES_TO_ICON_TYPES } from "@app/constants/datasetTypes";
import { clearResourceTreeByName as clearResourceTreeByNameAction } from "@app/actions/resources/tree";

import {
  CONTAINER_ENTITY_TYPES,
  DATASET_ENTITY_TYPES,
} from "@app/constants/Constants";
import { getIsStarred } from "@app/components/Tree/resourceTreeUtils.ts";

import DragSource from "components/DragComponents/DragSource";
import EllipsedText from "components/EllipsedText";
import { typeToIconType } from "@app/constants/DataTypes";
import { PureEntityIcon } from "@app/pages/HomePage/components/EntityIcon";
import FontIcon from "@app/components/Icon/FontIcon";
import TreeNodeBranchPicker from "@app/components/Tree/components/TreeNodeBranchPicker/TreeNodeBranchPicker";
import { icon as iconCls } from "@app/uiTheme/less/DragComponents/ColumnMenuItem.less";
import { selectState } from "@app/selectors/nessie/nessie";

import HoverDatasetInfoBox from "./HoverDatasetInfoBox";
import { IconButton, Tooltip } from "dremio-ui-lib";
import Spinner from "../Spinner";
import Message from "../Message";

import "./ResourceTree.less";
import "./TreeNode.less";
import "@app/components/Dataset/DatasetItemLabel.less";
import { getSourceByName, isBranchSelected } from "@app/utils/nessieUtils";
import { getNodeBranchId } from "./resourceTreeUtils";

export const TreeNode = (props) => {
  const {
    node,
    isNodeExpanded,
    isDatasetsDisabled,
    isSourcesHidden,
    shouldAllowAdd,
    selectedNodeId,
    dragType,
    shouldShowOverlay,
    addtoEditor,
    handleSelectedNodeChange,
    formatIdFromNode,
    isNodeExpandable,
    isExpandable,
    starredItems,
    starNode,
    unstarNode,
    isStarredLimitReached,
    selectedStarredTab,
    branchName,
    nessieSource,
    fromModal,
    currentNode,
    loadingItems,
    hideDatasets,
    hideSpaces,
    hideSources,
    hideHomes,
    clearResourceTreeByName,
  } = props;

  const [showOverlay, setShowOverlay] = useState(false); // eslint-disable-line @typescript-eslint/no-unused-vars
  const [nodeIsInProgress, setNodeIsInProgress] = useState(false);
  const [nodeError, setNodeError] = useState(false);
  const [errorMessage, setErrorMessage] = useState(
    intl.formatMessage({ id: "Message.Code.WS_CLOSED.Message" })
  );
  const [loadingTimer, setLoadingtimer] = useState(null);

  useEffect(() => {
    handleLoadingState();
  }, [loadingItems, selectedStarredTab]); // eslint-disable-line react-hooks/exhaustive-deps

  const clearSelection = (node) => {
    const nodeName = node.get("name");
    clearResourceTreeByName(nodeName);
    // DX-53668 - Tree not refreshing when switching branches
    if (isNodeExpanded(node)) {
      handleSelectedNodeChange(node, isNodeExpanded(node));
    }
    handleSelectedNodeChange(null, null); // Reset selection when inside of modal
  };

  const nessieDisabled = fromModal && nessieSource && !branchName;
  const saveAsDisabled =
    fromModal &&
    !["SOURCE", "SPACE", "HOME", "FOLDER"].includes(node.get("type"));
  const isDisabledNode = nessieDisabled || saveAsDisabled;

  let fullpath = node.get("fullPath");
  fullpath = fullpath.size > 1 ? fullpath.join(".") : null;
  const isBaseNode =
    (node.get("fullPath") && node.get("fullPath").size === 1) ||
    node.get("baseNode");

  const handleLoadingState = () => {
    let nodeId = node.get("id");
    // Accomodate for Starred items where duplicate nodes could exist
    if (node.get("branchId") !== undefined) {
      nodeId = getNodeBranchId(node);
    }
    if (loadingItems && loadingItems.get(nodeId)) {
      if (loadingItems.getIn([nodeId, "isInProgress"])) {
        // Delay 1000ms before showing loading spinner if API is taking long to respond
        setLoadingtimer(
          setTimeout(() => {
            setNodeIsInProgress(true);
          }, 200)
        );
      } else {
        // Remove loader if there is a failed status, but keep the item in the loading list
        // to retrieve error message from API
        clearTimeout(loadingTimer);
        setNodeIsInProgress(false);
      }

      // Set the error message if there's an error with the API callback
      if (
        loadingItems.getIn([nodeId, "isFailed"]) &&
        loadingItems.getIn([nodeId, "error"])
      ) {
        setNodeError(true);
        setErrorMessage(
          loadingItems.getIn([nodeId, "error", "message", "errorMessage"])
        );
      }
    } else {
      // Remove loader if item is removed from loading list
      clearTimeout(loadingTimer);
      setNodeIsInProgress(false);
      setNodeError(false);
    }
  };

  const renderResources = () => {
    if (isDisabledNode) return null;
    if (nodeError) {
      return <Message messageType="error" message={errorMessage} />;
    } else {
      return (node.get("resources") || Immutable.List()).map(
        (resource, index) => {
          if (
            (hideDatasets && DATASET_ENTITY_TYPES.has(resource.get("type"))) ||
            (hideSpaces && resource.get("type") === "SPACE") ||
            (hideHomes && resource.get("type") === "HOME") ||
            (hideSources && resource.get("type") === "SOURCE")
          ) {
            return;
          }

          return (
            <TreeNode
              node={resource}
              key={index}
              renderNode={renderNode}
              isNodeExpanded={isNodeExpanded}
              selectedNodeId={selectedNodeId}
              formatIdFromNode={formatIdFromNode}
              isDatasetsDisabled={isDatasetsDisabled}
              isSourcesHidden={isSourcesHidden}
              shouldAllowAdd={shouldAllowAdd}
              dragType={dragType}
              shouldShowOverlay={shouldShowOverlay}
              addtoEditor={addtoEditor}
              handleSelectedNodeChange={handleSelectedNodeChange}
              isNodeExpandable={isNodeExpandable}
              isExpandable={isExpandable}
              starredItems={starredItems}
              starNode={starNode}
              unstarNode={unstarNode}
              isStarredLimitReached={isStarredLimitReached}
              currentNode={currentNode}
              loadingItems={loadingItems}
              hideDatasets={hideDatasets}
              hideSpaces={hideSpaces}
              hideSources={hideSources}
              hideHomes={hideHomes}
              fromModal={fromModal}
            />
          );
        }
      );
    }
  };

  const renderNode = (nodeToRender, containerRef) => {
    if (
      (hideDatasets && DATASET_ENTITY_TYPES.has(nodeToRender.get("type"))) ||
      (hideSpaces && nodeToRender.get("type") === "SPACE") ||
      (hideHomes && nodeToRender.get("type") === "HOME") ||
      (hideSources && nodeToRender.get("type") === "SOURCE") ||
      (isSourcesHidden && nodeToRender.get("type") === "SOURCE")
    ) {
      return;
    }
    const nodeId = formatIdFromNode(nodeToRender);
    const isDisabled =
      isDatasetsDisabled &&
      !CONTAINER_ENTITY_TYPES.has(nodeToRender.get("type"));
    const arrowIconType =
      !isDisabledNode && isNodeExpanded(nodeToRender)
        ? "interface/down-chevron"
        : "interface/right-chevron";
    const arrowAlt = isNodeExpanded(nodeToRender) ? "Undisclosed" : "Disclosed";
    const isStarred = getIsStarred(starredItems, nodeToRender.get("id"));
    const unstarredWording = intl.formatMessage(
      isStarredLimitReached
        ? { id: "Resource.Tree.Starred.Limit.Reached" }
        : { id: "Resource.Tree.Add.Star" }
    );
    const unstarredAltText = intl.formatMessage(
      isStarredLimitReached
        ? { id: "Resource.Tree.Starred.Limit.Reached" }
        : { id: "Resource.Tree.Add.Star.Alt" }
    );

    const iconBlock = shouldAllowAdd ? (
      <>
        {node.get("isColumnItem") && node.get("isSorted") && (
          <Tooltip title="Tooltip.Icon.Sorted">
            <dremio-icon
              name="interface/sort"
              dataQa="is-partitioned"
              alt="sorted"
              class={iconCls}
            />
          </Tooltip>
        )}
        {node.get("isColumnItem") && node.get("isPartitioned") && (
          <Tooltip title="Tooltip.Icon.Partitioned">
            <dremio-icon
              name="sql-editor/partition"
              dataQa="is-partitioned"
              alt="partitioned"
              class={iconCls}
            />
          </Tooltip>
        )}
        {
          // need to add a empty placeholder for partition icon to keep alignment
          node.get("isColumnItem") &&
            node.get("isSorted") &&
            !node.get("isPartitioned") && <div className={iconCls}></div>
        }

        <IconButton
          tooltip="Tooltip.SQL.Editor.Add"
          onClick={() => {
            const elementToAdd = node.get("isColumnItem")
              ? node.get("name")
              : nodeToRender.get("fullPath");
            addtoEditor(elementToAdd);
          }}
          className="resourceTreeNode__add"
        >
          <dremio-icon name="interface/add-small" />
        </IconButton>
        {isNodeExpandable(nodeToRender) && nodeToRender.get("id") && (
          <IconButton
            tooltip={
              isStarred
                ? intl.formatMessage({ id: "Resource.Tree.Added.Star" })
                : unstarredWording
            }
            onClick={() => {
              if (!isStarred && !isStarredLimitReached) {
                starNode(nodeToRender.get("id"));
              } else if (isStarred) {
                unstarNode(nodeToRender.get("id"));
              }
            }}
            className={
              isStarred
                ? "resourceTreeNode__starIcon"
                : `resourceTreeNode__starIcon resourceTreeNode${
                    isStarredLimitReached ? "--limitReached" : "--unstarred"
                  }`
            }
          >
            <dremio-icon
              name={
                isStarred
                  ? "interface/star-starred"
                  : "interface/star-unstarred"
              }
              alt={
                isStarred
                  ? intl.formatMessage({ id: "Resource.Tree.Added.Star" })
                  : unstarredAltText
              }
            />
          </IconButton>
        )}
      </>
    ) : null;

    const activeClass = selectedNodeId === nodeId ? "active-node" : "";
    const nodeStatus = nodeToRender.getIn(["state", "status"], null);
    const isColumnItem = node.get("isColumnItem");

    const nodeElement = (
      <div
        className="resourceTreeNode node"
        onClick={() => {
          if (isDisabledNode) return;
          handleSelectedNodeChange(
            nodeToRender,
            isNodeExpanded(nodeToRender, nodeError)
          );
        }}
      >
        <Tooltip
          disableHoverListener={!nessieDisabled}
          title={intl.formatMessage({ id: "Nessie.SelectionDisabledHint" })}
        >
          <div
            {...(isDisabledNode && {
              style: {
                ...style.disabled,
                pointerEvents: "all",
                background: "transparent",
              },
            })}
            className="resourceTreeNode-nameWrapper"
          >
            {isColumnItem ? (
              <FontIcon
                type={typeToIconType[node.get("type")]}
                style={style.icon}
              />
            ) : (
              <PureEntityIcon
                entityType={node.get("type")}
                sourceStatus={nodeStatus}
                sourceType={nessieSource?.type}
                style={style.icon}
              />
            )}
            <Tooltip placement="top" title={node.get("name")}>
              <EllipsedText
                className="node-text"
                style={style.text}
                text={node.get("name")}
              />
            </Tooltip>
          </div>
        </Tooltip>
        {!!nessieSource && (
          <TreeNodeBranchPicker
            source={nessieSource}
            anchorRef={containerRef}
            onApply={() => {
              clearSelection(node);
            }}
          />
        )}
      </div>
    );

    const nodeWrap = (
      <div
        data-qa={nodeToRender.get("name")}
        onMouseUp={(e) => e.preventDefault()}
        style={isDisabled ? style.disabled : {}}
        className={classNames(
          "resourceTreeNodeWrap",
          activeClass,
          isColumnItem && "__columnItem"
        )}
      >
        {isNodeExpandable(nodeToRender) &&
          (nodeIsInProgress ? (
            <Spinner iconStyle={style.iconSpinner} style={style.spinnerBase} />
          ) : (
            <IconButton
              aria-label="Expand Node"
              onClick={(e) => {
                e.preventDefault();
                handleSelectedNodeChange(
                  nodeToRender,
                  isNodeExpanded(nodeToRender, nodeError)
                );
              }}
              className={classNames("TreeNode", "TreeNode__arrowIcon", {
                "TreeNode--disabled": isDisabledNode,
              })}
            >
              <dremio-icon
                name={arrowIconType}
                alt={intl.formatMessage({ id: `Common.${arrowAlt}` })}
              />
            </IconButton>
          ))}
        {nodeElement}
        <div className="resourceTreeNode__iconBlock">{iconBlock}</div>
      </div>
    );

    const nodeWrapWithOverlay = (
      <>
        {nodeWrap}
        {showOverlay ? (
          <HoverDatasetInfoBox
            classname={
              showOverlay ? "datasetInfoBox" : "datasetInfoBox--noShow"
            }
            node={nodeToRender}
            nodeStatus={nodeStatus}
            fullPath={fullpath}
          />
        ) : null}
      </>
    );

    return dragType ? (
      <div>
        <DragSource
          dragType={dragType}
          id={isColumnItem ? node.get("name") : nodeToRender.get("fullPath")}
          key={isColumnItem ? `${nodeId}-${node.get("name")}` : nodeId}
          className={activeClass}
        >
          {nodeWrapWithOverlay}
        </DragSource>
      </div>
    ) : (
      <div>{nodeWrapWithOverlay}</div>
    );
  };

  const nodeRef = useRef(null);
  const isDataset = Object.prototype.hasOwnProperty.call(
    DATASET_TYPES_TO_ICON_TYPES,
    node.get("type")
  );
  return (
    <div
      className={classNames("treeNode", {
        "treeNode--hasResources": isDataset,
        "treeNode--isBaseNode": isBaseNode,
        "treeNode--isLeafNode": !isBaseNode,
        "treeNode--isColumnItem": !!node.get("isColumnItem"),
        "treeNode--isDisabledNode": isDisabledNode,
      })}
    >
      <span ref={nodeRef}>{renderNode(node, nodeRef)}</span>
      {isNodeExpanded(node) && renderResources()}
    </div>
  );
};

TreeNode.propTypes = {
  node: PropTypes.instanceOf(Immutable.Map),
  sources: PropTypes.instanceOf(Immutable.List),
  isNodeExpanded: PropTypes.func,
  selectedNodeId: PropTypes.string,
  isDatasetsDisabled: PropTypes.bool,
  isSourcesHidden: PropTypes.bool,
  shouldAllowAdd: PropTypes.bool,
  dragType: PropTypes.any,
  shouldShowOverlay: PropTypes.bool,
  addtoEditor: PropTypes.func,
  handleSelectedNodeChange: PropTypes.func,
  formatIdFromNode: PropTypes.func,
  isNodeExpandable: PropTypes.any,
  isExpandable: PropTypes.any,
  starredItems: PropTypes.array,
  starNode: PropTypes.func,
  unstarNode: PropTypes.func,
  isStarredLimitReached: PropTypes.bool,
  selectedStarredTab: PropTypes.string,
  branchName: PropTypes.string,
  nessieSource: PropTypes.object,
  fromModal: PropTypes.bool,
  viewState: PropTypes.object,
  starViewState: PropTypes.object,
  currentNode: PropTypes.object,
  loadingItems: PropTypes.object,
  hideDatasets: PropTypes.bool,
  hideSpaces: PropTypes.bool,
  hideSources: PropTypes.bool,
  hideHomes: PropTypes.bool,
  clearResourceTreeByName: PropTypes.func,
};

function mapStateToProps(state, { node, sources }) {
  const nessieState = selectState(state.nessie, node.get("name"));
  return {
    branchName: isBranchSelected(nessieState)
      ? nessieState.reference.name
      : null,
    nessieSource: getSourceByName(node.get("name"), sources.toJS()),
  };
}

const mapDispatchToProps = {
  clearResourceTreeByName: clearResourceTreeByNameAction,
};

export default connect(mapStateToProps, mapDispatchToProps)(TreeNode);

const style = {
  arrow: {
    width: 24,
    height: 24,
  },
  emptySource: {
    paddingLeft: 30,
    fontSize: 12,
  },
  failed: {
    color: "red",
  },
  spinner: {
    position: "relative",
    display: "block",
    paddingLeft: 30,
  },
  spinnerIconStyle: {
    width: 25,
    height: 25,
  },
  disabled: {
    opacity: 0.7,
    pointerEvents: "none",
    color: "rgb(153, 153, 153)",
    background: "rgb(255, 255, 255)",
  },
  node: {
    display: "inline-flex",
    flexDirection: "row",
    justifyContent: "flex-start",
    alignItems: "center",
    width: "100%",
    cursor: "pointer",
  },
  emptyDiv: {
    height: 24,
    width: 15,
    marginLeft: 5,
    flex: "0 0 auto",
  },
  icon: {
    Container: {
      width: 21,
      height: 21,
    },
    Icon: {
      width: 21,
      height: 21,
    },
  },
  type: {
    Icon: {
      width: 24,
      height: 24,
      backgroundPosition: "center",
    },
    Container: {
      width: 24,
      height: 24,
      top: 0,
      flex: "0 0 auto",
    },
  },
  text: {
    marginLeft: 5,
    lineHeight: "21px",
  },
  spinnerBase: {
    position: "relative",
    width: 24,
    height: 24,
  },
  iconSpinner: {
    marginRight: 0,
  },
};
