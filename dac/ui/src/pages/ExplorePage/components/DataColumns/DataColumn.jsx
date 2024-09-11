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
import classNames from "clsx";
import { formatMessage } from "utils/locale";
import {
  typeToIconType,
  typeToFormatMessageId,
} from "@app/constants/DataTypes";
import {
  name as nameCls,
  icon as iconCls,
  wrapper,
  wrapperHover,
  nameWiki,
  iconWiki,
} from "./DataColumn.less";

import HighlightedColumnName from "./HighlightedColumnName";

export const columnPropTypes = {
  type: PropTypes.string, //see constants/DataTypes for the list of available types
  name: PropTypes.string,
};

export class DataColumn extends Component {
  static propTypes = {
    ...columnPropTypes,
    className: PropTypes.string,
    detailsView: PropTypes.bool,
    searchTerm: PropTypes.string,
  };

  render() {
    const { type, name, className, detailsView, searchTerm } = this.props;
    const label = `data-types/${typeToIconType[type]}`;
    const alt = typeToFormatMessageId?.[type] ?? typeToFormatMessageId["ANY"];

    return (
      <div
        className={classNames(detailsView ? wrapper : wrapperHover, className)}
      >
        <dremio-icon
          name={label}
          data-qa={label}
          alt={formatMessage(alt)}
          class={detailsView ? iconWiki : iconCls}
        />
        <div className={detailsView ? nameWiki : nameCls}>
          <HighlightedColumnName columnName={name} searchTerm={searchTerm} />
        </div>
      </div>
    );
  }
}
