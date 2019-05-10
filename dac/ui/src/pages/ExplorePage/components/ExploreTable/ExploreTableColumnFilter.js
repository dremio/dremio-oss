/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import pureRender from 'pure-render-decorator';
import { connect } from 'react-redux';
import { Link } from 'react-router';
import { withRouter } from 'react-router';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { SearchField } from 'components/Fields';
import { getTableColumns } from '@app/selectors/explore';
import exploreUtils from 'utils/explore/exploreUtils';
import { getColumnFilter } from 'selectors/explore';
import { updateColumnFilter } from 'actions/explore/view';
import { PageTypes } from 'pages/ExplorePage/pageTypes';
import { changePageTypeInUrl } from 'pages/ExplorePage/pageTypeUtils';

import { columnFilterWrapper, searchField, columnStats } from './ExploreTableColumnFilter.less';

@pureRender
export class ExploreTableColumnFilter extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    columnFilter: PropTypes.string,
    updateColumnFilter: PropTypes.func,
    columnCount: PropTypes.number,
    filteredColumnCount: PropTypes.number,
    //withRouter props
    location: PropTypes.object.isRequired
  };

  updateColumnFilter = (columnFilter) => {
    this.props.updateColumnFilter(columnFilter);
  };

  render() {
    const { columnFilter, columnCount, filteredColumnCount, location } = this.props;
    const wikiPath = changePageTypeInUrl(location.pathname, PageTypes.wiki) + location.search;

    return (
      <div className={columnFilterWrapper} data-qa='columnFilter'>
        <SearchField
          value={columnFilter}
          onChange={this.updateColumnFilter}
          className={searchField}
          placeholder={la('Column filter')}
          dataQa='explore-column-filter'
        />
        <div
          title={la('shown/all columns')}
          className={columnStats}
          data-qa='columnFilterStats'>
          {columnFilter && <span data-qa='columnFilterCount'>{filteredColumnCount} of </span>}
          <Link to={wikiPath}>{columnCount}</Link> {la('fields')}
        </div>
      </div>
    );
  }

}

function mapStateToProps(state, props) {
  const location = state.routing.locationBeforeTransitions || {};
  const datasetVersion = props.dataset.get('datasetVersion');
  const columns = getTableColumns(state, datasetVersion, location);
  const columnFilter = getColumnFilter(state);

  return {
    columnFilter,
    columnCount: columns.size,
    filteredColumnCount: exploreUtils.getFilteredColumnCount(columns, columnFilter)
  };
}

export default withRouter(connect(mapStateToProps, {updateColumnFilter})(ExploreTableColumnFilter));
