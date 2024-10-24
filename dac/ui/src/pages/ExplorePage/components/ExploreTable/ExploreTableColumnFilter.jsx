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
import { withRouter } from "react-router";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import { SearchField } from "components/Fields";
import { getColumnFilter, getTableColumns } from "selectors/explore";
import { updateColumnFilter } from "actions/explore/view";

import exploreUtils from "#oss/utils/explore/exploreUtils";
import { compose } from "redux";
import {
  columnFilterWrapper,
  searchField,
} from "./ExploreTableColumnFilter.less";

export class ExploreTableColumnFilter extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    columnFilter: PropTypes.string,
    updateColumnFilter: PropTypes.func,
    intl: PropTypes.any,
    disabled: PropTypes.bool,
  };

  updateColumnFilter = (columnFilter) => {
    this.props.updateColumnFilter(
      columnFilter,
      this.props.dataset.get("datasetVersion"),
    );
  };

  render() {
    const {
      columnFilter,
      disabled,
      intl: { formatMessage },
    } = this.props;

    return (
      <div className={columnFilterWrapper} data-qa="columnFilter">
        <SearchField
          value={columnFilter}
          onChange={this.updateColumnFilter}
          className={searchField}
          placeholder={formatMessage({ id: "Explore.SearchFilter" })}
          dataQa="explore-column-filter"
          disabled={disabled}
        />
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const location = state.routing.locationBeforeTransitions || {};
  const datasetVersion = props.dataset.get("datasetVersion");
  const columns = getTableColumns(state, datasetVersion, location);
  const columnFilter = getColumnFilter(state, datasetVersion);

  return {
    columnFilter,
    columnCount: columns.size,
    filteredColumnCount: exploreUtils.getFilteredColumnCount(
      columns,
      columnFilter,
    ),
  };
}

export default compose(
  connect(mapStateToProps, { updateColumnFilter }),
  injectIntl,
  withRouter,
)(ExploreTableColumnFilter);
