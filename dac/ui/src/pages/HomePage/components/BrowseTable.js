/*
 * Copyright (C) 2017 Dremio Corporation
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
import { PropTypes, Component } from 'react';
import Immutable from 'immutable';
import { debounce } from 'lodash/function';
import Mousetrap from 'mousetrap';
import invariant from 'invariant';

import * as allSpacesStyles from 'uiTheme/radium/allSpacesAndAllSources';
import { h3 } from 'uiTheme/radium/typography';
import EllipsedText from 'components/EllipsedText';

import { SearchField } from 'components/Fields';
import StatefulTableViewer from 'components/StatefulTableViewer';
import {SortDirection} from 'components/VirtualizedTableViewer';

import { constructFullPath } from 'utils/pathUtils';
import { tableStyles } from '../tableStyles';
import './BrowseTable.less';

export default class BrowseTable extends Component {
  static propTypes = {
    tableData: PropTypes.instanceOf(Immutable.List),
    columns: PropTypes.array,
    title: PropTypes.node,
    buttons: PropTypes.node,
    children: PropTypes.node,
    filterKey: PropTypes.string.isRequired
    // extra props passed along to underlying Table impl
  };

  static defaultProps = {
    filterKey: 'name',

    // pass thru to Table
    defaultSortBy: 'name',
    defaultSortDirection: SortDirection.ASC
  }

  state = {
    filter: ''
  };

  constructor(props) {
    super(props);

    this.handleFilterChange = debounce(this.handleFilterChange, 200);
  }

  componentDidMount() {
    Mousetrap.bind(['command+f', 'ctrl+f'], () => {
      this.searchField.focus();
      return false;
    });
  }

  componentWillUnmount() {
    Mousetrap.unbind(['command+f', 'ctrl+f']);
  }

  handleFilterChange = (filter) => {
    this.setState({
      filter
    });
  }

  filteredTableData = () => {
    const { filter } = this.state;
    return !filter ? this.props.tableData : this.props.tableData.filter((item) => {
      let value = item.data[this.props.filterKey].value;
      value = typeof value !== 'function' ? value : value.call(item.data[this.props.filterKey].value);
      return value.toLowerCase().includes(filter.trim().toLowerCase());
    });
  }

  render() {
    const { title, buttons, filterKey, tableData, ...passAlongProps } = this.props; // eslint-disable-line no-unused-vars
    invariant(
      !title || typeof title === 'string' || title.props.fullPath,
      'BrowseTable title must be string or BreadCrumbs.'
    );

    const resetScrollTop = Boolean(
      window.navigator.userAgent.toLowerCase().includes('firefox') &&
      this.state.filter
    ); //it's needed for https://dremio.atlassian.net/browse/DX-7140

    if (tableData.size) {
      passAlongProps.noDataText = la(`No items found for search “${this.state.filter}”.`); // todo: text sub loc
    }

    return (
      <div className='main-info'>
        {this.props.children}
        <div className='list-content' style={allSpacesStyles.main}>
          <div className='row'>
            <div style={allSpacesStyles.header} className='browse-table-viewer-header'>
              <h3 style={{...styles.heading}}>
                <EllipsedText text={
                  !title || typeof title === 'string'
                    ? title
                    : title && title.props && LRE + constructFullPath(title.props.fullPath.toJS(), true)
                }>
                  {LRE}{title}
                </EllipsedText>
              </h3>
              <div style={{display: 'flex', alignItems: 'center'}}>
                <SearchField
                  value={this.state.filter}
                  ref={searchField => this.searchField = searchField}
                  onChange={this.handleFilterChange}
                  style={tableStyles.searchField}
                  placeholder={la('Search...')}
                  showCloseIcon
                  inputClassName='mousetrap'
                />
                {buttons}
              </div>
            </div>
            <div className='move-info-wrap' style={{
              ...allSpacesStyles.height,
              overflow: 'hidden' // for FF (no worries, subsection will scroll)
            }}>
              <div className='table-wrap' style={allSpacesStyles.listContent}>
                <StatefulTableViewer
                  virtualized
                  className='table'
                  tableData={this.filteredTableData()}
                  resetScrollTop={resetScrollTop}
                  {...passAlongProps}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}


const styles = { // todo: RTL support
  heading: {
    ...h3,
    flexShrink: 1,
    minWidth: 0,
    direction: 'rtl' // use RTL mode as a hack to make truncation start at the left...
  }
};
const LRE = '\u202A'; // ... but make sure the text is treated as LTR by the text engine (e.g. render '@dremio', not 'dremio@')
// note: wrapping in <div> with direction:ltr doesn't produce "..."

