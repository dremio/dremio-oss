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
import { Cell } from "fixed-data-table-2";
import { SelectView } from "@app/components/Fields/SelectView";

import Radium from "radium";
import PropTypes from "prop-types";

import DragSource from "components/DragComponents/DragSource";
import ColumnActionMenu from "components/Menus/ExplorePage/ColumnActionMenu";
import ColumnTypeMenu from "components/Menus/ExplorePage/ColumnTypeMenu";
import FontIcon from "components/Icon/FontIcon";
import { overlay } from "@app/uiTheme/radium/overlay";

import { EXPLORE_HOVER_COLOR } from "uiTheme/radium/colors";

import { typeToIconType, BINARY, MIXED } from "@app/constants/DataTypes";
import Keys from "@app/constants/Keys.json";

const MAX_COLUMN_NAME_LENTH = 62;
const ACTION_MENU_WIDTH = 24;
const COLUMN_HEIGHT = 24;
const MARGIN_RIGHT = 5;

export class ColumnHeader extends PureComponent {
  static propTypes = {
    pageType: PropTypes.string,
    defaultColumnWidth: PropTypes.number,
    isResizeInProgress: PropTypes.bool,
    column: PropTypes.object.isRequired,
    width: PropTypes.number,
    dragType: PropTypes.string.isRequired,
    pathname: PropTypes.string,
    query: PropTypes.object,
    isDumbTable: PropTypes.bool,
    columnsCount: PropTypes.number,
    isEdited: PropTypes.bool,

    updateColumnName: PropTypes.func.isRequired,
    makeTransform: PropTypes.func.isRequired,
    openDetailsWizard: PropTypes.func,
    preconfirmTransform: PropTypes.func.isRequired,
  };

  forceFocus = false; // eslint-disable-line react/sort-comp

  static emulateAutoPosition(
    anchor,
    target,
    targetOrigin,
    anchorOrigin,
    targetPosition
  ) {
    const move = targetPosition.top + target.bottom - window.innerHeight;
    return {
      top: move > 0 ? targetPosition.top - move - 10 : targetPosition.top,
      bottom: 0,
      left: targetPosition.left,
    };
  }

  static getDragData(name) {
    return {
      type: "columnName",
      data: { name },
    };
  }

  doTypeAction(type) {
    if (this.isActionsPrevented()) {
      return false;
    }
    const { column } = this.props;
    if (type === "MIXED") {
      this.props.openDetailsWizard({
        detailType: "SINGLE_DATA_TYPE",
        columnName: column.name,
      });
    } else {
      this.setState({
        openType: true,
        anchorElType: event.currentTarget,
      });
    }
  }

  isActionsPrevented() {
    return (
      this.props.pageType !== "default" ||
      this.props.isDumbTable ||
      this.props.isEdited
    );
  }

  handleFocus = () => {
    // WARNING This is a bit tricky
    // When ReactModal opens, it saves and blurs the current input.
    // Then, when it closes, it refocuses the saved input, which will trigger this handler again.
    // To avoid this messy situation, blur the input before starting preconfirm, then refocus it on confirm.

    if (!this.forceFocus) {
      // timeout to let the focus resolve before blurring. This allows it to keep the caret position.
      setTimeout(() => {
        if (this.input) {
          this.input.blur();
        }
        return this.props.preconfirmTransform().then(() => {
          this.forceFocus = true;
          if (this.input) {
            this.input.focus();
          }
          //IE fix: timeout so forceFocus will be set to false after next handleFocus call
          setTimeout(() => {
            this.forceFocus = false;
          }, 0);
          return null;
        });
      }, 0);
    }
  };

  handleUpdateColumnName(name, e) {
    if (e.target.value) {
      this.props.updateColumnName(name, e);
    }
  }

  handleKeyPress(name, e) {
    // todo: switch to KeyboardEvent.code (w/ polyfill)
    if (e.keyCode === Keys.ENTER) {
      // removed call to handleUpdateColumnName to prevent double-firing transformAndPreview
      this.input.blur();
    } else if (e.keyCode === Keys.ESCAPE) {
      this.input.value = name;
      this.input.blur();
      e.preventDefault();
    }
  }

  handleRenameAction = () => {
    if (this.input) {
      this.input.focus();
    }
  };

  renderEditableColumnName(column, label, cellWidth) {
    const style = {
      width: !this.isActionsPrevented()
        ? cellWidth - MAX_COLUMN_NAME_LENTH + MARGIN_RIGHT
        : cellWidth - MAX_COLUMN_NAME_LENTH + ACTION_MENU_WIDTH + MARGIN_RIGHT,
      userSelect: this.props.isResizeInProgress ? "none" : "initial",
      ...styles.inputStyle,
      textDecoration: column.status === "DELETED" ? "line-through" : "none",
    };
    return (
      <input
        className="cell"
        ref={(input) => (this.input = input)}
        type="text"
        disabled={this.isActionsPrevented() || this.props.isResizeInProgress}
        autoComplete="off"
        data-lpignore="true" // for lastpass: DX-9664 Password auto-complete icons show up in our column headers
        style={style}
        id={`cell${column.name}`}
        contentEditable
        onFocus={this.handleFocus}
        onKeyDown={this.handleKeyPress.bind(this, column.name)}
        onBlur={this.handleUpdateColumnName.bind(this, column.name)}
        defaultValue={label}
      />
    );
  }

  renderColumnIcon(type, label) {
    if (type === "?") {
      return (
        <span className="type" id={`${label} + type`} style={styles.other}>
          ?
        </span>
      );
    }

    const { isDumbTable, openDetailsWizard, column } = this.props;
    const canClick =
      !this.isActionsPrevented() && !isDumbTable && type !== BINARY; // disable binary type conversions pending DX-5159

    const iconProps = {
      type: typeToIconType[type],
      tooltip: canClick ? la("Change type") : la("Data type"),
      theme: styles.typeColumn,
      id: `${label} + type`,
      class: "type",
    };

    if (!canClick) {
      return <FontIcon {...iconProps} style={{ cursor: "default" }} />;
    }

    if (type === MIXED) {
      return (
        <FontIcon
          {...iconProps}
          onClick={() =>
            openDetailsWizard({
              detailType: "SINGLE_DATA_TYPE",
              columnName: column.name,
            })
          }
        />
      );
    }

    return (
      <SelectView
        content={<FontIcon {...iconProps} />}
        hideExpandIcon
        useLayerForClickAway={false}
        listStyle={styles.popoverAnimation}
      >
        {({ closeDD }) => (
          <div style={styles.popover}>
            <ColumnTypeMenu
              columnType={column.type}
              columnName={column.name}
              hideDropdown={closeDD}
              openDetailsWizard={openDetailsWizard}
              makeTransform={this.props.makeTransform}
            />
          </div>
        )}
      </SelectView>
    );
  }

  renderActionMenuIcon(column) {
    if (this.isActionsPrevented()) return null;

    return (
      <SelectView
        content={<dremio-icon name="interface/more" class="action-menu-icon" />}
        hideExpandIcon
        listRightAligned
        useLayerForClickAway={false}
        listStyle={styles.popoverAnimation}
      >
        {({ closeDD }) => (
          <div style={styles.popover}>
            <ColumnActionMenu
              columnType={column.type}
              columnName={column.name}
              hideDropdown={closeDD}
              columnsCount={this.props.columnsCount}
              openDetailsWizard={this.props.openDetailsWizard}
              makeTransform={this.props.makeTransform}
              disabledButtons={[]}
              onRename={this.handleRenameAction}
            />
          </div>
        )}
      </SelectView>
    );
  }

  render() {
    const { column, width } = this.props;
    const label = column.name;
    const type = column.type;
    return (
      <Cell style={styles.themeStyle}>
        <DragSource
          nativeDragData={ColumnHeader.getDragData(label)}
          preventDrag={this.props.isResizeInProgress}
          dragType={this.props.dragType}
          id={label}
        >
          <div data-qa={label} key={label} style={styles.wrapperColumn}>
            <div style={styles.colWrap}>
              {this.renderColumnIcon(type, label)}
              {this.renderEditableColumnName(column, label, width)}
            </div>
            {this.renderActionMenuIcon(column)}
          </div>
        </DragSource>
      </Cell>
    );
  }
}

const styles = {
  inputStyle: {
    height: COLUMN_HEIGHT,
    border: "none",
    position: "static",
    alignText: "center",
    backgroundColor: "transparent",
    marginRight: 0,
  },
  popoverAnimation: {
    // need this because of the case where group by or join button overlays column action menu
    zIndex: overlay.zIndex + 1, // to show a menu above spinner, as spinner should not block headers anymore. Previous value was 2
    transition: `transform 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                 opacity 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                 top 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms`,
  },
  colWrap: {
    display: "flex",
  },
  typeMixed: {
    Icon: {
      height: 16,
      width: 16,
      position: "relative",
      color: "#FFBB57",
    },
  },
  typeLabel: {
    cursor: "pointer",
    textTransform: "capitalize",
    paddingTop: 5,
  },
  popover: {
    padding: 0,
    width: 168,
  },
  typeColumn: {
    Icon: {
      height: 16,
      width: 16,
      backgroundPosition: "center",
    },
  },
  wrapperColumn: {
    position: "static",
    display: "flex",
    justifyContent: "space-between",
  },
  other: {
    position: "relative",
    top: 3,
    marginLeft: 5,
    marginRight: 3,
    cursor: "pointer",
    opacity: 0.7,
  },
  arrowDown: {
    Container: {
      position: "static",
      float: "right",
      height: 25,
      cursor: "pointer",
      ":hover": {
        backgroundColor: EXPLORE_HOVER_COLOR,
      },
    },
  },
};
export default Radium(ColumnHeader);
