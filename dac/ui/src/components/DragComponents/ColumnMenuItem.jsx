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
import classNames from "clsx";
import { injectIntl } from "react-intl";

import EllipsedText from "components/EllipsedText";
import { typeToIconType } from "#oss/constants/DataTypes";
import { constructFullPath } from "utils/pathUtils";

import { IconButton, Tooltip } from "dremio-ui-lib";
import {
  itemContainer,
  base,
  content,
  disabled as disabledCls,
  iconInDropdown as iconCls,
  datasetAdd,
} from "#oss/uiTheme/less/DragComponents/ColumnMenuItem.less";
import DragSource from "./DragSource";

class ColumnMenuItem extends PureComponent {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    disabled: PropTypes.bool,
    fullPath: PropTypes.instanceOf(Immutable.List),
    dragType: PropTypes.string.isRequired,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    type: PropTypes.string,
    index: PropTypes.number,
    nativeDragData: PropTypes.object,
    preventDrag: PropTypes.bool,
    name: PropTypes.string,
    fieldType: PropTypes.string,
    className: PropTypes.string,
    intl: PropTypes.any,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func,
    draggableRowClassName: PropTypes.string,
    showReadonlyTooltip: PropTypes.bool,
  };
  static defaultProps = {
    fullPath: Immutable.List(),
    showReadonlyTooltip: true,
  };

  checkThatDragAvailable = (e) => {
    if (this.props.preventDrag || this.props.disabled) {
      e.stopPropagation();
      e.preventDefault();
    }
  };

  render() {
    const {
      item,
      disabled,
      preventDrag,
      fieldType,
      className,
      shouldAllowAdd,
      addtoEditor,
      draggableRowClassName,
      showReadonlyTooltip,
      intl: { formatMessage },
    } = this.props;
    const markAsDisabled = preventDrag || disabled;
    const isGroupBy = this.props.dragType === "groupBy";
    // full paths are not yet supported by dremio in SELECT clauses, so force this to always be the simple name for now
    const idForDrag =
      true || // eslint-disable-line no-constant-condition
      isGroupBy
        ? item.get("name")
        : constructFullPath(this.props.fullPath.concat(item.get("name")));
    return (
      <div
        className={classNames(["inner-join-left-menu-item", base, className])}
        onMouseDown={this.checkThatDragAvailable}
      >
        <DragSource
          nativeDragData={this.props.nativeDragData}
          dragType={this.props.dragType}
          type={this.props.type}
          index={this.props.index}
          onDragStart={this.props.handleDragStart}
          onDragEnd={this.props.onDragEnd}
          preventDrag={preventDrag}
          isFromAnother
          id={idForDrag}
        >
          <div
            className={classNames([
              "draggable-row",
              content,
              markAsDisabled && disabledCls,
              draggableRowClassName,
            ])}
            data-qa={`inner-join-field-${item.get("name")}-${fieldType}`}
          >
            <dremio-icon
              class="mr-05 icon-primary"
              name={`data-types/${typeToIconType[item.get("type")]}`}
            ></dremio-icon>
            <div className={itemContainer}>
              <EllipsedText
                data-qa={item.get("name")}
                style={
                  !preventDrag
                    ? { paddingRight: 10 }
                    : {} /* leave space for knurling */
                }
                text={item.get("name")}
                title={
                  preventDrag && showReadonlyTooltip
                    ? formatMessage({ id: "Read.Only" })
                    : item.get("name")
                }
              >
                {item.get("name")}
              </EllipsedText>
            </div>
            {/*
              We need to put sort and partition icons to columns, i.e. ('S' and 'P' below represents
              the icons):
              col1  S |P
              col2  S |
              col3    |P
              col4  S |P
            */}
            {item.get("isSorted") && (
              <Tooltip title="Tooltip.Icon.Sorted">
                <dremio-icon
                  name="interface/sort"
                  dataQa="is-partitioned"
                  alt="sorted"
                  class={iconCls}
                />
              </Tooltip>
            )}
            {item.get("isPartitioned") && (
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
              item.get("isSorted") && !item.get("isPartitioned") && (
                <div className={iconCls}></div>
              )
            }
            {shouldAllowAdd && (
              <IconButton
                tooltip="Tooltip.SQL.Editor.Add"
                onClick={() => addtoEditor(item.get("name"))}
                className={datasetAdd}
              >
                <dremio-icon name="interface/add-small" />
              </IconButton>
            )}
          </div>
        </DragSource>
      </div>
    );
  }
}

export default injectIntl(ColumnMenuItem);
