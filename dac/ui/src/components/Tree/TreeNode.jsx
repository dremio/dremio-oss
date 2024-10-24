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
import { useContext, useEffect, useRef, useState, memo, useMemo } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import classNames from "clsx";
import { intl } from "#oss/utils/intl";
import { DATASET_TYPES_TO_ICON_TYPES } from "#oss/constants/datasetTypes";
import { clearResourceTreeByName as clearResourceTreeByNameAction } from "#oss/actions/resources/tree";
import {
  CONTAINER_ENTITY_TYPES,
  DATASET_ENTITY_TYPES,
} from "#oss/constants/Constants";
import {
  getFullPathFromResourceTree,
  getIsStarred,
} from "#oss/components/Tree/resourceTreeUtils.ts";

import DragSource from "components/DragComponents/DragSource";
import EllipsedText from "components/EllipsedText";
import { typeToIconType } from "#oss/constants/DataTypes";
import { PureEntityIcon } from "#oss/pages/HomePage/components/EntityIcon";
import { icon as iconCls } from "#oss/uiTheme/less/DragComponents/ColumnMenuItem.less";
import { selectState } from "#oss/selectors/nessie/nessie";

import { Popover, IconButton, Tooltip } from "dremio-ui-lib/components";
import Spinner from "../Spinner";
import Message from "../Message";

import "./ResourceTree.less";
import "./TreeNode.less";
import "#oss/components/Dataset/DatasetItemLabel.less";
import { isBranchSelected, getSourceById } from "#oss/utils/nessieUtils";
import { getNodeBranchId } from "./resourceTreeUtils";
import DatasetSummaryOverlay from "../Dataset/DatasetSummaryOverlay";
import { ARSFeatureSwitch } from "@inject/utils/arsUtils";
import { getExploreState } from "#oss/selectors/explore";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import SourceBranchPicker from "#oss/pages/HomePage/components/SourceBranchPicker/SourceBranchPicker";
import { TreeConfigContext } from "./treeConfigContext";
import { getSourceMap } from "#oss/selectors/home";
import EntitySummaryOverlay from "../EntitySummaryOverlay/EntitySummaryOverlay";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

// Rendering the popover is slow when there are many tree nodes, only render when focused or hovered
const WrappedPopover = ({ children, ...rest }) => {
  const [shouldRender, setShouldRender] = useState(false);

  return (
    <div
      onFocus={() => !shouldRender && setShouldRender(true)}
      onMouseEnter={() => !shouldRender && setShouldRender(true)}
      style={{
        display: "contents",
      }}
    >
      {shouldRender ? <Popover {...rest}>{children}</Popover> : children}
    </div>
  );
};

export const TreeNode = (props) => {
  const {
    node,
    isNodeExpanded,
    isDatasetsDisabled,
    isSourcesHidden,
    shouldAllowAdd,
    selectedNodeId,
    dragType,
    shouldShowOverlay = true,
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
    nessieSource: nessieSourceProp,
    fromModal,
    currentNode,
    loadingItems,
    hideDatasets,
    hideSpaces,
    hideSources,
    hideHomes,
    clearResourceTreeByName,
    isMultiQueryRunning,
    level = 0,
    isManageAccessEnabled,
  } = props;
  const nessieSource = useMemo(() => {
    // Pass the toJS'd version to children so that this doesn't have to be called again
    if (nessieSourceProp && "toJS" in nessieSourceProp) {
      return nessieSourceProp.toJS();
    } else {
      return nessieSourceProp;
    }
  }, [nessieSourceProp]);

  const {
    nessiePrefix,
    restrictSelection,
    handleDatasetDetails: openDatasetDetails,
  } = useContext(TreeConfigContext);

  const [nodeIsInProgress, setNodeIsInProgress] = useState(false);
  const [nodeError, setNodeError] = useState(false);
  const [errorMessage, setErrorMessage] = useState(
    intl.formatMessage({ id: "Message.Code.WS_CLOSED.Message" }),
  );
  const { t } = getIntlContext();
  const [loadingTimer, setLoadingtimer] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [hovered, setHovered] = useState(false);
  const [hideIcons, setHideIcons] = useState(false);

  useEffect(() => {
    handleLoadingState();
  }, [loadingItems, selectedStarredTab]); // eslint-disable-line react-hooks/exhaustive-deps

  const isBadState = nessieSource && nessieSource.state?.status !== "good";
  const nessieDisabled = restrictSelection && nessieSource && !branchName;
  const saveAsDisabled =
    restrictSelection &&
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
          }, 200),
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
          loadingItems.getIn([nodeId, "error", "message", "errorMessage"]),
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
            <TreeNodeMemo
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
              nessieSource={nessieSource}
              branchName={branchName}
              isMultiQueryRunning={isMultiQueryRunning}
              openDatasetDetails={openDatasetDetails}
              level={level + 1}
            />
          );
        },
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
        : { id: "Resource.Tree.Add.Star" },
    );
    const unstarredAltText = intl.formatMessage(
      isStarredLimitReached
        ? { id: "Resource.Tree.Starred.Limit.Reached" }
        : { id: "Resource.Tree.Add.Star.Alt" },
    );

    const showSummaryOverlay = shouldShowOverlay;
    const isEntity =
      !nodeToRender.get("type").includes("PHYSICAL_DATASET") &&
      !nodeToRender.get("type").includes("VIRTUAL_DATASET");
    const nodeQueryInfo = isEntity
      ? {
          getEntityUrl: () => getFullPathFromResourceTree(node),
          fromTreeNode: true,
          entityId: nodeToRender.get("id"),
        }
      : {
          versionContext: nodeToRender.get("id"),
          type: nodeToRender.get("type"),
          id: nodeToRender.get("id"),
          entityId: nodeToRender.get("id"),
          fullPath: nodeToRender.get("fullPath"),
          fromTreeNode: true,
        };

    const hideDatasetPanelIcon = node.get("isColumnItem");

    const iconBlock =
      hovered && shouldAllowAdd ? (
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

          {!hideDatasetPanelIcon && (
            <IconButton
              tooltipPortal
              tooltip={intl.formatMessage({ id: "Open.Details" })}
              onClick={() => {
                openDatasetDetails(
                  Immutable.fromJS({
                    ...nodeQueryInfo,
                  }),
                );
              }}
              className="resourceTreeNode__add"
            >
              <dremio-icon name="interface/meta" />
            </IconButton>
          )}

          <IconButton
            tooltipPortal
            tooltip={intl.formatMessage({ id: "Tooltip.SQL.Editor.Add" })}
            onClick={() => {
              const elementToAdd = node.get("isColumnItem")
                ? node.get("name")
                : nodeToRender.get("fullPath");
              addtoEditor(elementToAdd);
            }}
            disabled={isMultiQueryRunning}
            className="resourceTreeNode__add"
          >
            <dremio-icon name="interface/add-small" />
          </IconButton>
          <ARSFeatureSwitch
            renderEnabled={() => null}
            renderDisabled={() =>
              isNodeExpandable(nodeToRender) &&
              nodeToRender.get("id") && (
                <IconButton
                  tooltipPortal
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
                          isStarredLimitReached
                            ? "--limitReached"
                            : "--unstarred"
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
              )
            }
          />
        </>
      ) : null;

    const activeClass = selectedNodeId === nodeId ? "active-node" : "";
    const nodeStatus = nodeToRender.getIn(["state", "status"], null);
    const isColumnItem = node.get("isColumnItem");
    const isSource = node.get("type") === "SOURCE";

    const treeNodeContent = (
      <div
        {...(isDisabledNode && {
          style: style.disabledNode,
        })}
        className="resourceTreeNode-nameWrapper"
      >
        {isColumnItem ? (
          <dremio-icon
            class="icon-primary"
            name={`data-types/${typeToIconType[node.get("type")]}`}
            style={style.icon24}
          ></dremio-icon>
        ) : (
          <PureEntityIcon
            tooltipPortal
            disableHoverListener={
              !hovered || nessieDisabled || showSummaryOverlay
            }
            entityType={node.get("type")}
            sourceStatus={nodeStatus}
            sourceType={nessieSource?.type}
            style={style.icon}
            enableTooltip={hovered}
            isManageAccessEnabled={isManageAccessEnabled}
          />
        )}
        {nessieDisabled || showSummaryOverlay ? (
          <EllipsedText
            className="node-text ml-05"
            style={style.text}
            text={node.get("name")}
          />
        ) : hovered ? (
          <Tooltip placement="top" content={node.get("name")}>
            <EllipsedText
              className="node-text ml-05"
              style={style.text}
              text={node.get("name")}
            />
          </Tooltip>
        ) : (
          <EllipsedText
            className="node-text ml-05"
            style={style.text}
            text={node.get("name")}
          />
        )}
      </div>
    );

    const hideOverlay = !hovered || fromModal || hideDatasets;

    const tooltipElement = (
      <>
        {!isDragging && !hideOverlay ? (
          !showSummaryOverlay ? (
            <Tooltip
              shouldWrapChildren
              placement="bottom"
              title={
                isBadState
                  ? intl.formatMessage({
                      id: "Source.NotAvailable",
                    })
                  : intl.formatMessage({
                      id: "Nessie.SelectionDisabledHint",
                    })
              }
              disableHoverListener={!nessieDisabled}
            >
              {treeNodeContent}
            </Tooltip>
          ) : (
            <WrappedPopover
              onOpen={() => setHideIcons(true)}
              onClose={() => setHideIcons(false)}
              role="tooltip"
              showArrow
              delay={750}
              placement="right"
              mode="hover"
              portal
              content={
                isEntity ? (
                  <EntitySummaryOverlay
                    name={node.get("name")}
                    type={node.get("type").toLowerCase()}
                    fullPath={node.get("fullPath")}
                    getEntityUrl={() => getFullPathFromResourceTree(node)}
                    openDetailsPanel={openDatasetDetails}
                  />
                ) : (
                  <DatasetSummaryOverlay
                    inheritedTitle={nodeToRender.get("fullPath").last()}
                    fullPath={nodeToRender.get("fullPath")}
                    openWikiDrawer={openDatasetDetails}
                    hideSqlEditorIcon
                    versionContext={getVersionContextFromId(
                      nodeToRender.get("id"),
                    )}
                  />
                )
              }
            >
              {treeNodeContent}
            </WrappedPopover>
          )
        ) : (
          <>{treeNodeContent}</>
        )}
      </>
    );

    const nodeElement = (
      <div
        className="resourceTreeNode node"
        onClick={() => {
          if (isDisabledNode) return;
          handleSelectedNodeChange(
            nodeToRender,
            isNodeExpanded(nodeToRender, nodeError),
          );
        }}
      >
        {tooltipElement}
        {nodeToRender.get("metadataOutdated") && !isColumnItem && (
          <Tooltip
            portal
            placement="top"
            content={t("Sonar.Dataset.DataGraph.OutdatedWarning")}
            style={{ width: 300 }}
          >
            <dremio-icon
              name="interface/warning"
              alt="Warning"
              style={{
                paddingLeft: 4,
                width: 24,
                height: 24,
                color: "var(--fill--warning--solid)",
              }}
            />
          </Tooltip>
        )}
        {isSource && !!nessieSource && (
          <SourceBranchPicker
            redirect={false}
            source={nessieSource}
            anchorRef={() => containerRef.current}
            onApply={(stateKey, { reference, hash }) => {
              // Do not select node in save as dialog since tags/commits are not supported
              if (
                restrictSelection &&
                (reference?.type.toUpperCase() !== "BRANCH" || !!hash)
              ) {
                handleSelectedNodeChange(null, null);
                return;
              }

              // Select node after clicking apply on branch picker, clear the children in redux also
              clearResourceTreeByName(node.get("name"), fromModal);
              const isExpanded = isNodeExpanded(nodeToRender, nodeError);
              if (!isExpanded) return;

              handleSelectedNodeChange(nodeToRender, isExpanded);

              setImmediate(() => {
                handleSelectedNodeChange(nodeToRender, !isExpanded);
              });
            }}
            prefix={nessiePrefix}
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
          isColumnItem && "__columnItem",
        )}
      >
        {nodeIsInProgress && (
          <Spinner iconStyle={style.iconSpinner} style={style.spinnerBase} />
        )}
        {isNodeExpandable(nodeToRender) && !nodeIsInProgress && (
          <IconButton
            aria-label="Expand Node"
            onClick={(e) => {
              e.preventDefault();
              handleSelectedNodeChange(
                nodeToRender,
                isNodeExpanded(nodeToRender, nodeError),
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
        )}
        {nodeElement}
        <div
          className={classNames(
            "resourceTreeNode__iconBlock",
            hideIcons && "hide",
          )}
        >
          {iconBlock}
        </div>
      </div>
    );

    return dragType ? (
      <div>
        <DragSource
          dragType={dragType}
          onDrag={setIsDragging}
          id={isColumnItem ? node.get("name") : nodeToRender.get("fullPath")}
          key={isColumnItem ? `${nodeId}-${node.get("name")}` : nodeId}
          className={activeClass}
        >
          {nodeWrap}
        </DragSource>
      </div>
    ) : (
      <div>{nodeWrap}</div>
    );
  };

  const nodeRef = useRef(null);
  const isDataset = Object.prototype.hasOwnProperty.call(
    DATASET_TYPES_TO_ICON_TYPES,
    node.get("type"),
  );

  return (
    <>
      <div
        {...(!hideIcons && {
          onMouseEnter: () => setHovered(true),
          onMouseLeave: () => setHovered(false),
        })}
        aria-level={level}
        className={classNames("treeNode", {
          "treeNode--hasResources": isDataset,
          "treeNode--isBaseNode": isBaseNode,
          "treeNode--isLeafNode": !isBaseNode,
          "treeNode--isColumnItem": !!node.get("isColumnItem"),
          "treeNode--isDisabledNode": isDisabledNode,
        })}
      >
        <span ref={nodeRef}>{renderNode(node, nodeRef)}</span>
      </div>
      {isNodeExpanded(node) && renderResources()}
    </>
  );
};

TreeNode.propTypes = {
  node: PropTypes.instanceOf(Immutable.Map),
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
  isMultiQueryRunning: PropTypes.bool,
  openDatasetDetails: PropTypes.func,
};

const TreeNodeMemo = memo(TreeNode);

function mapStateToProps(state, { node }) {
  const explorePageState = getExploreState(state);
  const nessieState = selectState(state.nessie, node.get("name"));
  return {
    branchName: isBranchSelected(nessieState)
      ? nessieState.reference.name
      : null,
    isMultiQueryRunning: explorePageState?.view.isMultiQueryRunning,
    nessieSource: getSourceById(node.get("id"), getSourceMap(state)),
  };
}

const mapDispatchToProps = {
  clearResourceTreeByName: clearResourceTreeByNameAction,
};

export default connect(mapStateToProps, mapDispatchToProps)(TreeNodeMemo);

const style = {
  icon24: {
    inlineSize: 24,
    blockSize: 24,
  },
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
  disabledNode: {
    opacity: 0.7,
    pointerEvents: "none",
    color: "rgb(153, 153, 153)",
    background: "rgb(255, 255, 255)",
    pointerEvents: "all",
    background: "transparent",
  },
  node: {
    display: "inline-flex",
    flexDirection: "row",
    justifyContent: "flex-start",
    alignItems: "center",
    width: "100%",
    cursor: "pointer",
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
