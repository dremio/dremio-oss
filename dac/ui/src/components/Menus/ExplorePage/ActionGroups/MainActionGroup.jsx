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

import { MIXED } from "@app/constants/DataTypes";
import {
  ALL_TYPES,
  CONVERTIBLE_TYPES,
  NOT_LIST_AND_MAP_TYPES,
} from "@app/constants/columnTypeGroups";
import ColumnMenuItem from "./../ColumnMenus/ColumnMenuItem";

class MainActionGroup extends PureComponent {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    columnType: PropTypes.string,
    columnsCount: PropTypes.number,
  };
  render() {
    const { columnType, columnsCount } = this.props;
    return (
      <div>
        <ColumnMenuItem
          actionType="RENAME"
          columnType={columnType}
          title={laDeprecated("Rename…")}
          availableTypes={ALL_TYPES}
          onClick={this.props.makeTransform}
        />
        <ColumnMenuItem
          actionType="DROP"
          columnType={columnType}
          title={laDeprecated("Drop")}
          disabled={columnsCount === 1}
          availableTypes={ALL_TYPES}
          onClick={this.props.makeTransform}
        />
        <ColumnMenuItem
          actionType="CONVERT_DATA_TYPE"
          columnType={columnType}
          title={laDeprecated("Convert Data Type…")}
          availableTypes={CONVERTIBLE_TYPES}
          onClick={this.props.makeTransform}
        />
        <ColumnMenuItem
          actionType="GROUP_BY"
          columnType={columnType}
          title={laDeprecated("Group By…")}
          availableTypes={NOT_LIST_AND_MAP_TYPES}
          onClick={this.props.makeTransform}
        />
        <ColumnMenuItem
          actionType="SINGLE_DATA_TYPE"
          columnType={columnType}
          title={laDeprecated("Single Data Type…")}
          availableTypes={[MIXED]}
          onClick={this.props.makeTransform}
        />
        <ColumnMenuItem
          actionType="SPLIT_BY_DATA_TYPE"
          columnType={columnType}
          title={laDeprecated("Split by Data Type…")}
          availableTypes={[MIXED]}
          onClick={this.props.makeTransform}
        />
      </div>
    );
  }
}
export default MainActionGroup;
