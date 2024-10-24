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

import Select from "components/Fields/Select";

import { bodySmall, formDefault } from "uiTheme/radium/typography";
import { LINE_CENTER_CENTER } from "uiTheme/radium/flexStyle";
import {
  MAP,
  LIST,
  OTHER,
  GEO,
  MIXED,
  ANY,
  STRUCT,
} from "#oss/constants/DataTypes";
import JoinColumnMenu from "./components/JoinColumnMenu";
import JoinDragArea from "./components/JoinDragArea";
import { ddList as ddListCls, ddItem as ddItemCls } from "./InnerJoin.less";

const DEFAULT_WIDTH = 200;

export const NOT_SUPPORTED_TYPES = new Set([
  MAP,
  LIST,
  OTHER,
  GEO,
  MIXED,
  ANY,
  STRUCT,
]);

export class InnerJoin extends Component {
  static propTypes = {
    dragColumntableType: PropTypes.string,
    leftColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    rightColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    removeColumn: PropTypes.func.isRequired,
    addColumnToInnerJoin: PropTypes.func.isRequired,
    stopDrag: PropTypes.func.isRequired,
    onDragStart: PropTypes.func.isRequired,
    handleDrop: PropTypes.func.isRequired,
    addEmptyColumnToInnerJoin: PropTypes.func.isRequired,
    dragType: PropTypes.string.isRequired,
    type: PropTypes.string,
    fields: PropTypes.object,
    defaultNameForDisplay: PropTypes.string,
    customNameForDisplay: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    columnDragName: PropTypes.string,
    columnsInDragArea: PropTypes.instanceOf(Immutable.List),
    canSelect: PropTypes.any,
  };

  leftDisabledColumnNames = undefined;
  rightDisabledColumnNames = undefined;

  constructor(props) {
    super(props);

    this.items = [
      {
        label: "Inner",
        value: "Inner",
        des: "Only matching records",
        icon: "sql-editor/join-inner",
      },
      {
        label: "Left Outer",
        value: "LeftOuter",
        des: "All records from left, matching records from right",
        icon: "sql-editor/join-left",
      },
      {
        label: "Right Outer",
        value: "RightOuter",
        des: "All records from right, matching records from left",
        icon: "sql-editor/join-right",
      },
      {
        label: "Full Outer",
        value: "FullOuter",
        des: "All records from both",
        icon: "sql-editor/join-full",
      },
    ];
    this.receiveProps(props, {});
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, oldProps) {
    // disabledColumnNames is wholly derived from these props, so only recalculate it when one of them has changed
    if (nextProps.columnsInDragArea !== oldProps.columnsInDragArea) {
      this.leftDisabledColumnNames = this.getDisabledColumnNames(
        nextProps,
        true,
      );
      this.rightDisabledColumnNames = this.getDisabledColumnNames(
        nextProps,
        false,
      );
    }
  }
  getDisabledColumnNames(props, isLeftSide) {
    const columns = isLeftSide ? props.leftColumns : props.rightColumns;
    const columnsInDragArea = Immutable.Set(
      props.columnsInDragArea.map((col) =>
        col.getIn([isLeftSide ? "default" : "custom", "name"]),
      ),
    );
    const disabledColumns = columns.filter(
      (column) =>
        NOT_SUPPORTED_TYPES.has(column.get("type")) ||
        columnsInDragArea.has(column.get("name")),
    );
    return Immutable.Set(disabledColumns.map((column) => column.get("name")));
  }

  getDragPart() {
    const props = {
      addColumn: this.props.addColumnToInnerJoin,
      leftColumns: this.props.leftColumns,
      rightColumns: this.props.rightColumns,
      removeColumn: this.props.removeColumn,
      onDragStart: this.props.onDragStart,
      handleDrop: this.props.handleDrop,
      columnDragName: this.props.columnDragName,
      dragType: this.props.dragType,
      isDragInProgress: this.props.isDragInProgress,
      type: this.props.dragType,
      items: this.props.columnsInDragArea,
    };
    if (this.props.columnsInDragArea.size) {
      return (
        <div style={styles.dragOneWrapStyles}>
          <JoinDragArea
            dragColumntableType={this.props.dragColumntableType}
            {...props}
          />
        </div>
      );
    }
    return (
      <div style={styles.dragWrapStyles}>
        <JoinDragArea {...props} />
        <JoinDragArea {...props} />
      </div>
    );
  }

  renderCurItem = (selectedItem) => {
    if (!selectedItem) return "";
    return (
      <div style={styles.label}>
        <div style={LINE_CENTER_CENTER}>
          <dremio-icon
            style={{ inlineSize: 24, blockSize: 24 }}
            name={selectedItem.icon}
          ></dremio-icon>
          {selectedItem.label}
        </div>
      </div>
    );
  };

  renderDdItem = (item) => {
    return <DdItem {...item} />;
  };

  render() {
    const joinType = this.props.fields.joinType;
    const { canSelect } = this.props;
    return (
      <div
        className="inner-join"
        style={styles.base}
        onMouseUp={this.props.stopDrag}
      >
        <div style={styles.wrap}>
          <div style={styles.item}>
            <span style={styles.font}>{laDeprecated("Type: ")}</span>
            <Select
              dataQa="selectedJoinType"
              items={this.items}
              valueField="value"
              itemRenderer={this.renderDdItem}
              value={joinType.value}
              selectedValueRenderer={this.renderCurItem}
              onChange={joinType.onChange}
              style={ddStyles.button}
              listClass={ddListCls}
              itemClass={ddItemCls}
            />
          </div>
        </div>
        <div style={styles.inner}>
          <JoinColumnMenu
            type="default"
            columns={this.props.leftColumns}
            disabledColumnNames={this.leftDisabledColumnNames}
            onDragEnd={this.props.stopDrag}
            handleDragStart={this.props.onDragStart}
            dragType={this.props.dragType}
            nameForDisplay={this.props.defaultNameForDisplay}
            canSelect={canSelect}
          />
          {this.getDragPart()}
          <JoinColumnMenu
            type="custom"
            columns={this.props.rightColumns}
            disabledColumnNames={this.rightDisabledColumnNames}
            onDragEnd={this.props.stopDrag}
            handleDragStart={this.props.onDragStart}
            dragType={this.props.dragType}
            nameForDisplay={this.props.customNameForDisplay}
            canSelect={canSelect}
          />
        </div>
        <div style={styles.center}>
          <div
            style={styles.add}
            onClick={this.props.addEmptyColumnToInnerJoin}
          >
            <dremio-icon name="interface/add"></dremio-icon>
            <span className="ml-05">
              {laDeprecated("Add a Join Condition")}
            </span>
          </div>
        </div>
      </div>
    );
  }
}

const DdItem = ({ label, icon, des }) => {
  const itemIcon = icon ? (
    <dremio-icon
      style={{ inlineSize: 24, blockSize: 24 }}
      name={icon}
    ></dremio-icon>
  ) : null;
  const primaryText = (
    <div style={ddStyles.defaultTextStyle}>
      {itemIcon}
      <div>{label}</div>
    </div>
  );
  const secondaryText = <div style={ddStyles.secondaryText}>{des}</div>;
  return (
    <div>
      {primaryText}
      {secondaryText}
    </div>
  );
};
DdItem.propTypes = {
  label: PropTypes.string,
  value: PropTypes.any,
  icon: PropTypes.string,
  des: PropTypes.string,
  isLast: PropTypes.bool,
};

const styles = {
  base: {
    flex: 1,
    display: "flex",
    minHeight: 180,
    flexWrap: "wrap",
    justifyContent: "center",
    backgroundColor: "var(--fill--brand)",
  },
  wrap: {
    width: "100%",
    display: "flex",
    paddingBottom: 5,
    height: 38,
    backgroundColor: "var(--fill--brand)",
  },
  inner: {
    width: "100%",
    backgroundColor: "var(--fill--primary)",
    justifyContent: "space-between",
    display: "flex",
    margin: "3px 10px",
    maxHeight: 180,
    minHeight: 180,
  },
  center: {
    width: "100%",
    height: 30,
    display: "flex",
    margin: "0 10px",
    alignItems: "center",
    borderBottom: `2px solid var(--border--neutral)`,
    justifyContent: "center",
    backgroundColor: "var(--fill--brand)",
    padding: "0 10px",
  },
  add: {
    display: "flex",
    alignItems: "center",
    cursor: "pointer",
  },
  rightMenu: {
    borderLeft: `2px solid var(--border--neutral)`,
  },
  dragWrapStyles: {
    display: "flex",
    width: "100%",
    justifyContent: "space-between",
  },
  dragOneWrapStyles: {
    display: "flex",
    width: "100%",
    overflowY: "auto",
    justifyContent: "space-between",
  },
  item: {
    maxWidth: 235,
    width: 235,
    alignItems: "center",
    marginLeft: 20,
    fontWeight: 400,
    position: "relative",
    marginTop: 5,
    display: "flex",
    flexDirection: "row",
    justifyContent: "flex-start",
  },
  font: {
    margin: "0 10px 0 -5px",
  },
  select: {
    padding: 0,
    width: DEFAULT_WIDTH,
    height: 28,
    marginTop: 2,
    marginLeft: 0,
    ...bodySmall,
  },
  dragArea: {
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    width: "100%",
    height: 180,
  },
  border: {
    borderLeft: "1px solid #ccc",
  },
  dragAreaText: {
    width: 180,
    color: "gray",
    fontSize: 12,
    textAlign: "center",
    display: "inline-block",
  },
  label: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    justifyContent: "space-between",
    width: "100%",
    ...formDefault,
  },
};

const ddStyles = {
  button: {
    height: 24,
    width: 180,
    boxShadow: 0,
  },
  wrapped: {
    marginLeft: -15,
    lineHeight: 2,
    height: 55,
    display: "flex",
    justifyContent: "flex-end",
    alignItems: "flex-start",
    flexDirection: "column-reverse",
  },
  secondaryText: {
    whiteSpace: "normal",
    marginTop: -5,
    lineHeight: "14px",
    marginLeft: 24,
    color: "#999999",
  },
  defaultTextStyle: {
    display: "flex",
    alignItems: "center",
    height: 27,
  },
};

export default InnerJoin;
