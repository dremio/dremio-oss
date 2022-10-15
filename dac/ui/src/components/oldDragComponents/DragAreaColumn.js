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
import PropTypes from "prop-types";
import Immutable from "immutable";
import clsx from "clsx";
import { Popover } from "@app/components/Popover";

import FontIcon from "components/Icon/FontIcon";

import { formDefault, formPlaceholder } from "uiTheme/radium/typography";
import { typeToIconType } from "@app/constants/DataTypes";
import { HISTORY_ITEM_COLOR } from "uiTheme/radium/colors";
import DragSource from "components/DragComponents/DragSource";
import DragTarget from "components/DragComponents/DragTarget";
import * as classes from "./DragAreaColumn.module.less";

const NOT_SUPPORTED_TYPES = ["MAP", "LIST"];

const COUNT_ACTION = "Count (*)";

class DragAreaColumn extends PureComponent {
  static propTypes = {
    dragColumntableType: PropTypes.string,
    ownDragColumntableType: PropTypes.string,
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    dragType: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    onDragStart: PropTypes.func,
    isDragInProgress: PropTypes.bool,
    moveColumn: PropTypes.func,
    removeColumn: PropTypes.func,
    index: PropTypes.number,
    showColumnDropdown: PropTypes.func,
    showSortDropdown: PropTypes.func,
    onDragEnd: PropTypes.func,
    addColumn: PropTypes.func,
    icon: PropTypes.any,
    id: PropTypes.any,
    columns: PropTypes.instanceOf(Immutable.List),
  };

  constructor(props) {
    super(props);
    this.renderContent = this.renderContent.bind(this);
    this.showPopover = this.showPopover.bind(this);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.handleDrop = this.handleDrop.bind(this);
    this.moveColumn = this.moveColumn.bind(this);
    this.disableColor = this.disableColor.bind(this);
    this.enableColor = this.enableColor.bind(this);
    this.checkDropPosibility = this.checkDropPosibility.bind(this);
    this.selectItemOnDrag = this.selectItemOnDrag.bind(this);

    this.state = {
      fieldDisabled: false,
      isOpen: false,
      pattern: "",
      isNotEmptyDragArea: false,
      anchorEl: null,
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.item.get("action") === COUNT_ACTION) {
      this.setState({
        fieldDisabled: true,
        isOpen: false,
        pattern: "",
      });
    } else {
      this.setState({
        fieldDisabled: false,
      });
    }
  }

  handleDrop(data) {
    const { type, id, item, index } = this.props;
    const selectedItem = {
      columnName: data.id,
      dragColumnId: id || item.get("name"),
      dragColumnType: type,
      dragColumnIndex: index,
    };
    if (this.checkDropPosibility()) {
      this.props.addColumn(selectedItem);
    }
  }

  selectItemOnDrag(selectedItem) {
    const { disabled } = selectedItem;
    if (disabled) {
      return;
    }
    this.handleRequestClose();
    this.props.addColumn(selectedItem);
  }

  moveColumn(...args) {
    if (this.props.moveColumn) {
      this.props.moveColumn(...args);
    }
  }

  enableColor() {
    if (this.checkDropPosibility()) {
      this.setState({ isNotEmptyDragArea: true });
    }
  }

  disableColor() {
    this.setState({ isNotEmptyDragArea: false });
  }

  showPopover(event) {
    if (!this.shouldHideField()) {
      this.setState({
        isOpen: true,
        anchorEl: event.currentTarget,
      });
    }
  }

  shouldHideField() {
    return this.props.item.get("action") === COUNT_ACTION;
  }

  checkDropPosibility() {
    const {
      item,
      isDragInProgress,
      dragColumntableType,
      ownDragColumntableType,
    } = this.props;
    return (
      item.get("empty") &&
      isDragInProgress &&
      dragColumntableType === ownDragColumntableType
    );
  }

  handleRequestClose() {
    this.setState({ isOpen: false });
  }

  handlePatternChange(e) {
    this.setState({
      pattern: e.target.value,
    });
  }

  renderColumns() {
    const { type, id, item, index } = this.props;
    const { pattern } = this.state;
    return this.props.columns
      .filter((column) =>
        column.get("name").toLowerCase().includes(pattern.trim().toLowerCase())
      )
      .sort((a) => NOT_SUPPORTED_TYPES.indexOf(a.get("type")) !== -1)
      .map((column) => {
        const columnType = column.get("type");
        const disabled = NOT_SUPPORTED_TYPES.indexOf(columnType) !== -1;
        const disabledStyle = disabled ? { color: HISTORY_ITEM_COLOR } : {};
        const columnName = column.get("name");
        const selectedItem = {
          dragColumnId: id || item.get("name"),
          dragColumnType: type,
          dragColumnIndex: index,
          columnName,
          columnType,
          disabled,
        };

        return (
          <div
            disabled={disabled}
            data-qa={columnName}
            style={formDefault}
            className={clsx(classes["column"])}
            key={columnName}
            onClick={this.selectItemOnDrag.bind(this, selectedItem)}
          >
            <FontIcon
              type={typeToIconType[columnType]}
              theme={themeStyles.type}
            />
            <span style={{ marginLeft: 5, ...disabledStyle }}>
              {columnName}
            </span>
          </div>
        );
      });
  }

  renderContent() {
    const { item, index, type } = this.props;
    const column = item;
    if (this.checkDropPosibility()) {
      return (
        <div
          className={clsx(
            classes["content"],
            classes["largeContent"],
            classes["empty"]
          )}
          key="custom-content"
          onClick={this.showPopover}
          data-qa={`join-search-field-area-${index}-${type}`}
        >
          <span>Drag a field here</span>
        </div>
      );
    } else if (column.get("empty")) {
      return (
        <div
          className={clsx(
            classes["content"],
            classes["largeContent"],
            classes["empty"]
          )}
          style={{ borderWidth: 0 }}
          key="custom-content"
          onClick={this.showPopover}
        >
          <div className={clsx(classes["searchWrap"])}>
            <input
              className={clsx(classes["search"])}
              style={{ ...formPlaceholder, fontSize: 11 }}
              onChange={this.handlePatternChange.bind(this)}
              placeholder={la("Choose field…")}
            />
            <FontIcon type="Search" theme={themeStyles.icon} />
          </div>
          <Popover
            useLayerForClickAway={false}
            anchorEl={this.state.isOpen ? this.state.anchorEl : null}
            onClose={this.handleRequestClose}
            listWidthSameAsAnchorEl
          >
            <div className={clsx(classes["popover"])} data-qa="popover">
              {this.renderColumns()}
            </div>
          </Popover>
        </div>
      );
    }
    return (
      <div
        className={clsx(classes["content"], classes["largeContent"])}
        key="custom-content"
      >
        <FontIcon
          type={typeToIconType[item.get("type")]}
          key="custom-type"
          theme={themeStyles.type}
        />
        <span className={clsx(classes["name"])} key="custom-name">
          {column.get("name")}
        </span>
      </div>
    );
  }

  render() {
    const { item, type, index } = this.props;
    const column = item;

    return (
      <div
        className={clsx(
          {
            [classes["opacity"]]: column.get("isFake"),
          },
          classes["base"],
          "inner-join-column"
        )}
        onDragLeave={this.disableColor}
        onDragOver={this.enableColor}
      >
        <DragSource
          dragType={this.props.dragType}
          onDragStart={this.onDragStart}
          index={this.props.index}
          onDragEnd={this.props.onDragEnd}
          id={column.get("name")}
        >
          <DragTarget
            onDrop={this.handleDrop}
            dragType={this.props.dragType}
            moveColumn={this.moveColumn}
            index={this.props.index}
            id={column.get("name")}
          >
            <div className={clsx(classes["columnWrap"])}>
              <div
                className={clsx(
                  {
                    [classes["dragStyle"]]: this.checkDropPosibility(),
                    [classes["dragstyle--empty"]]:
                      this.checkDropPosibility() &&
                      this.state.isNotEmptyDragArea,
                    [classes["hidden"]]: this.shouldHideField(),
                  },
                  classes["simpleColumn"],
                  classes["largeColumn"]
                )}
                key="custom"
              >
                {this.renderContent()}
              </div>
              {this.props.icon ? (
                this.props.icon
              ) : (
                <FontIcon
                  type="CanceledGray"
                  theme={themeStyles.fontIcon}
                  onClick={this.props.removeColumn.bind(
                    this,
                    type,
                    column.get("name"),
                    index
                  )}
                />
              )}
            </div>
          </DragTarget>
        </DragSource>
      </div>
    );
  }
}

const themeStyles = {
  icon: {
    Container: {
      position: "absolute",
      right: 5,
      top: 5,
    },
    Icon: {
      width: 18,
      height: 18,
    },
  },
  fontIcon: {
    Container: {
      width: 25,
      height: 25,
      position: "relative",
      top: 1,
    },
    Icon: {
      cursor: "pointer",
    },
  },
  type: {
    Icon: {
      width: 24,
      height: 20,
      backgroundPosition: "left center",
    },
    Container: {
      width: 24,
      height: 20,
      top: 0,
    },
  },
};

export default DragAreaColumn;
