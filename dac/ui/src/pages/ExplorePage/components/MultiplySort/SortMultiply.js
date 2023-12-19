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
import classNames from "clsx";

import DragColumnMenu from "components/DragComponents/DragColumnMenu";
import EllipsedText from "components/EllipsedText";
import SortDragArea from "./components/SortDragArea";

import * as classes from "./SortMultiply.module.less";
import "./SortMultiply.less";

// TODO: DRY and fix with Join/Group BY

class SortMultiply extends Component {
  static propTypes = {
    columnsField: PropTypes.array,
    dataset: PropTypes.instanceOf(Immutable.Map),
    isDragInProgress: PropTypes.bool,
    columns: PropTypes.instanceOf(Immutable.List).isRequired,
    path: PropTypes.string,
    handleDrop: PropTypes.func.isRequired,
    handleDragStart: PropTypes.func,
    handleDragStop: PropTypes.func,
    dragType: PropTypes.string.isRequired,
    addAnother: PropTypes.func,
  };

  getNamesOfColumnsInDragArea() {
    return Immutable.Set(
      this.props.columnsField.map((item) => item.name.value)
    );
  }

  render() {
    return (
      // todo: loc
      <div
        className={classNames(["inner-join", classes["base"]])}
        onMouseUp={this.props.handleDragStop}
      >
        <div className={classes["header"]}>
          <div className={classes["name"]}>
            <EllipsedText
              text={`“${this.props.dataset.getIn([
                "displayFullPath",
                -1,
              ])}” dataset columns:`}
            />
          </div>
          <div className={classes["name"]} style={{ borderRight: "none" }}>
            {laDeprecated("Sort columns:")}
          </div>
        </div>
        <div className={classNames(["inner-join__body", classes["inner"]])}>
          <DragColumnMenu
            items={this.props.columns || Immutable.List()}
            disabledColumnNames={this.getNamesOfColumnsInDragArea()}
            type="column"
            onDragEnd={this.props.handleDragStop}
            handleDragStart={this.props.handleDragStart}
            dragType={this.props.dragType}
            name={this.props.path + " <current>"}
            showSearchIcon
          />
          <SortDragArea
            columnsField={this.props.columnsField}
            allColumns={this.props.columns}
            onDrop={this.props.handleDrop}
            dragType={this.props.dragType}
            isDragInProgress={this.props.isDragInProgress}
          />
        </div>
        <span className={classes["add-join"]} onClick={this.props.addAnother}>
          <dremio-icon name="interface/add" />
          <span className={classes["add-text"]}>
            {laDeprecated("Add a Sort Column")}
          </span>
        </span>
      </div>
    );
  }
}

export default SortMultiply;
