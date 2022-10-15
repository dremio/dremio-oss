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
import { connect } from "react-redux";
import { intl } from "@app/utils/intl";
import PropTypes from "prop-types";
import Immutable from "immutable";
import classNames from "classnames";
import FontIcon from "components/Icon/FontIcon";
import TextHighlight from "components/TextHighlight";
import EllipsedText from "components/EllipsedText";
import { getStarredItemIds } from "selectors/tree";
import { getIsStarred } from "@app/components/Tree/resourceTreeUtils";
// need this util as MainInfoItemName.js wraps label into a link. If we do not block event bubbling
// redirect would occur
import { stopPropagation } from "@app/utils/reactEventUtils";
import { IconButton, Tooltip } from "dremio-ui-lib";
import DatasetOverlayContent from "./DatasetOverlayContent";
import DatasetSummaryOverlay from "./DatasetSummaryOverlay";
import "./DatasetItemLabel.less";

export class DatasetItemLabel extends PureComponent {
  static propTypes = {
    name: PropTypes.string, // defaults to last token of fullPath
    inputValue: PropTypes.string,
    fullPath: PropTypes.instanceOf(Immutable.List),
    showFullPath: PropTypes.bool,
    isNewQuery: PropTypes.bool,
    placement: PropTypes.string,
    customNode: PropTypes.node,
    dragType: PropTypes.string,
    typeIcon: PropTypes.string.isRequired,
    iconSize: PropTypes.oneOf(["MEDIUM", "LARGE"]),
    style: PropTypes.object,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func,
    isExpandable: PropTypes.bool,
    className: PropTypes.string,
    isStarred: PropTypes.bool,
    nodeId: PropTypes.string,
    setShowOverlay: PropTypes.func,
    starNode: PropTypes.func,
    unstarNode: PropTypes.func,
    isStarredLimitReached: PropTypes.bool,
    isSearchItem: PropTypes.bool,
    showSummaryOverlay: PropTypes.bool,
  };

  static defaultProps = {
    fullPath: Immutable.List(),
    showFullPath: false,
    iconSize: "MEDIUM",
    shouldShowOverlay: false,
  };

  state = {
    isOpenOverlay: false,
    isIconHovered: false,
    isDragInProgress: false,
    isLoadingData: false,
  };

  setOpenOverlay = () => this.setState({ isOpenOverlay: true });

  setCloseOverlay = () => this.setState({ isOpenOverlay: false });

  handleMouseEnterIcon = () => {
    this.setState({ isIconHovered: true });
  };
  toggleIsDragInProgress = () => {
    this.setState((prevState) => ({
      isDragInProgress: !prevState.isDragInProgress,
    }));
  };

  handleMouseLeaveIcon = () => {
    this.setState({ isIconHovered: false });
  };

  handleClick = (evt) => {
    if (!this.props.shouldShowOverlay || !this.props.isExpandable) return;
    stopPropagation(evt);
    this.setState((prevState) => ({ isOpenOverlay: !prevState.isOpenOverlay }));
  };

  renderDefaultNode() {
    let { name } = this.props;
    const {
      inputValue,
      fullPath,
      showFullPath,
      isSearchItem,
      showSummaryOverlay = true,
    } = this.props;
    const joinedPath = fullPath.slice(0, -1).join(".");

    if (!name && fullPath) {
      name = fullPath.last();
    }

    return (
      <div
        style={styles.datasetLabel}
        className={isSearchItem ? "search-wrapper" : ""}
      >
        <EllipsedText
          text={name}
          data-qa={name}
          className={isSearchItem ? "heading searchHeading" : "heading"}
        >
          <TextHighlight
            showTooltip={!showSummaryOverlay}
            text={name}
            inputValue={inputValue}
          />
        </EllipsedText>
        {showFullPath && (
          <EllipsedText
            text={joinedPath}
            className={isSearchItem ? "heading2 searchSubHeading" : "heading2"}
          >
            <TextHighlight
              showTooltip={!showSummaryOverlay}
              text={joinedPath}
              inputValue={inputValue}
            />
          </EllipsedText>
        )}
      </div>
    );
  }

  render() {
    const {
      fullPath,
      customNode,
      typeIcon,
      style,
      iconSize,
      isExpandable,
      className,
      shouldAllowAdd,
      addtoEditor,
      isStarred,
      nodeId,
      starNode,
      unstarNode,
      isStarredLimitReached,
      showSummaryOverlay,
    } = this.props;

    const { isOpenOverlay } = this.state;

    const iconStyle = iconSize === "LARGE" ? styles.largeIcon : {};
    const labelTypeIcon = iconSize === "LARGE" ? `${typeIcon}Large` : typeIcon;
    const node = customNode || this.renderDefaultNode();
    const arrowIconType = isOpenOverlay
      ? "interface/down-chevron"
      : "interface/right-chevron";
    const showInfoIcon = true;
    const unstarredWording = intl.formatMessage({
      id: `${
        isStarredLimitReached
          ? "Resource.Tree.Starred.Limit.Reached"
          : "Resource.Tree.Add.Star"
      }`,
    });
    const unstarredAltText = intl.formatMessage({
      id: `${
        isStarredLimitReached
          ? "Resource.Tree.Starred.Limit.Reached"
          : "Resource.Tree.Add.Star.Alt"
      }`,
    });

    const renderDataItemLabel = () => {
      return (
        <div
          data-qa="info-icon"
          style={{
            ...styles.iconsBase,
            ...(showInfoIcon && isExpandable && { cursor: "pointer" }),
          }}
          onMouseEnter={this.handleMouseEnterIcon}
          onMouseLeave={this.handleMouseLeaveIcon}
          onClick={this.handleClick}
          className={classNames(
            "datasetItemLabel-item__content",
            !isExpandable && "--isNotExpandable"
          )}
        >
          {isExpandable && (
            <dremio-icon name={arrowIconType} class="expand-icon" />
          )}
          <FontIcon
            type={labelTypeIcon}
            iconStyle={{
              ...iconStyle,
              verticalAlign: "middle",
              flexShrink: 0,
            }}
          />
          {node}
        </div>
      );
    };

    return (
      <div style={{ ...styles.base, ...(style || {}) }} className={className}>
        <div style={styles.list} className="datasetItemLabel">
          <div className="datasetItemLabel-item">
            {labelTypeIcon !== "Script" && labelTypeIcon !== "FileEmpty" ? (
              <Tooltip
                open={showSummaryOverlay}
                type="richTooltip"
                enterDelay={1000}
                title={
                  <DatasetSummaryOverlay
                    inheritedTitle={fullPath.last()}
                    fullPath={fullPath}
                  />
                }
              >
                {renderDataItemLabel()}
              </Tooltip>
            ) : (
              renderDataItemLabel()
            )}
            <div className="datasetItemLabel-item__iconBlock">
              {shouldAllowAdd && (
                <>
                  <IconButton
                    tooltip="Add to SQL editor"
                    onClick={() => addtoEditor(fullPath)}
                    className="datasetItemLabel-item__add"
                  >
                    <dremio-icon name="interface/add-small" />
                  </IconButton>
                  {nodeId && (
                    <IconButton
                      tooltip={
                        isStarred
                          ? intl.formatMessage({
                              id: "Resource.Tree.Added.Star",
                            })
                          : unstarredWording
                      }
                      onClick={() => {
                        if (!isStarred && !isStarredLimitReached) {
                          starNode(nodeId);
                        } else if (isStarred) {
                          unstarNode(nodeId);
                        }
                      }}
                      className={
                        isStarred
                          ? "datasetItemLabel-item__starIcon datasetItemLabel-item--starred"
                          : `datasetItemLabel-item__starIcon resourceTreeNode${
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
                            ? intl.formatMessage({
                                id: "Resource.Tree.Added.Star",
                              })
                            : unstarredAltText
                        }
                      />
                    </IconButton>
                  )}
                </>
              )}
            </div>
          </div>

          {this.state.isOpenOverlay && (
            <DatasetOverlayContent
              fullPath={fullPath}
              showFullPath
              placement={this.props.placement}
              dragType={this.props.dragType}
              toggleIsDragInProgress={this.toggleIsDragInProgress}
              typeIcon={typeIcon}
              onClose={this.setCloseOverlay}
              shouldAllowAdd={shouldAllowAdd}
              addtoEditor={addtoEditor}
              isStarredLimitReached={isStarredLimitReached}
            />
          )}
        </div>
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const { nodeId } = props;
  const starredList = getStarredItemIds(state);
  const isStarred = getIsStarred(starredList, nodeId);
  return {
    isStarred,
  };
}

export default connect(mapStateToProps)(DatasetItemLabel);

const styles = {
  base: {
    width: "100%",
  },
  datasetLabel: {
    minWidth: 0,
    marginBottom: 1,
    marginLeft: 5,
    paddingright: 10,
    flexGrow: 1,
  },
  largeIcon: {
    width: 40,
    height: 38,
  },
  iconsBase: {
    position: "relative",
    display: "flex",
    alignItems: "center",
    overflow: "hidden",
  },
  backdrop: {
    position: "fixed",
    top: 0,
    bottom: 0,
    right: 0,
    left: 0,
    height: "100%",
    width: "100%",
    zIndex: 1000,
  },
  infoTheme: {
    Icon: {
      width: 12,
      height: 12,
    },
    Container: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      width: 24,
      height: 24,
      position: "absolute",
      left: 0,
      top: 0,
      bottom: 0,
      right: 0,
      margin: "auto",
    },
  },
  list: {
    display: "flex",
    flexDirection: "column",
  },
};
