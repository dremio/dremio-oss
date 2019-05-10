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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { debounce } from 'lodash/function';
import Mousetrap from 'mousetrap';
import invariant from 'invariant';
import { injectIntl } from 'react-intl';

import EllipsedText from 'components/EllipsedText';

import { SearchField } from 'components/Fields';
import StatefulTableViewer from 'components/StatefulTableViewer';
import {SortDirection} from 'components/VirtualizedTableViewer';
import { WikiButton } from '@app/pages/HomePage/components/WikiButton';
import { BrowseTableResizer } from '@app/pages/HomePage/components/BrowseTableResizer';
import { Row, GridColumn, SidebarColumn } from '@app/pages/HomePage/components/Columns';

import { constructFullPath } from 'utils/pathUtils';
import { tableStyles } from '../tableStyles';
import { wikiCollapsed } from './BrowseTable.less';

@injectIntl
export default class BrowseTable extends Component {
  static propTypes = {
    rightSidebar: PropTypes.node,
    rightSidebarExpanded: PropTypes.bool,
    toggleSidebar: PropTypes.func,
    tableData: PropTypes.instanceOf(Immutable.List),
    columns: PropTypes.array,
    title: PropTypes.node,
    buttons: PropTypes.node,
    children: PropTypes.node,
    filterKey: PropTypes.string.isRequired,
    intl: PropTypes.object.isRequired
    // extra props passed along to underlying Table impl
  };

  static defaultProps = {
    filterKey: 'name',

    // pass thru to Table
    defaultSortBy: 'name',
    defaultSortDirection: SortDirection.ASC
  };

  state = {
    filter: ''
  };

  mainContainerNode = null;

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
  };

  filteredTableData = () => {
    const { filter } = this.state;
    return !filter ? this.props.tableData : this.props.tableData.filter((item) => {
      let value = item.data[this.props.filterKey].value;
      value = typeof value !== 'function' ? value : value.call(item.data[this.props.filterKey].value);
      return value.toLowerCase().includes(filter.trim().toLowerCase());
    });
  };

  onMainContainerRef = mainEl => {
    this.mainContainerNode = mainEl;
  };

  getMainContainer = () => this.mainContainerNode;

  render() {
    const {
      title,
      buttons,
      tableData,
      rightSidebar,
      rightSidebarExpanded,
      intl,
      toggleSidebar,
      ...passAlongProps
    } = this.props; // eslint-disable-line no-unused-vars
    invariant(
      !title || typeof title === 'string' || title.props.fullPath,
      'BrowseTable title must be string or BreadCrumbs.'
    );

    const resetScrollTop = Boolean(
      window.navigator.userAgent.toLowerCase().includes('firefox') &&
      this.state.filter
    ); //it's needed for https://dremio.atlassian.net/browse/DX-7140

    if (tableData.size) {
      passAlongProps.noDataText = intl.formatMessage({ id: 'Search.BrowseTable'}, { filter: this.state.filter });
    }

    return (
      <div className='main-info' ref={this.onMainContainerRef}>
        {this.props.children}
        <div className='list-content'>
          <div className='row'>
            <div>
              <Row className='browse-table-viewer-header'>
                <GridColumn style={{
                  justifyContent: 'space-between',
                  alignItems: 'center'
                }}>
                  <h3 style={styles.heading}>
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
                      placeholder={intl.formatMessage({ id: 'Dataset.SearchEllipsis' })}
                      showCloseIcon
                      inputClassName='mousetrap'
                      dataQa='browse-table-search'
                    />
                    {buttons}
                    {!rightSidebarExpanded && rightSidebar && <WikiButton
                      isSelected={rightSidebarExpanded}
                      onClick={toggleSidebar}
                      className={wikiCollapsed}
                    />}
                  </div>
                </GridColumn>
                {rightSidebarExpanded && rightSidebar && <SidebarColumn style={{
                  display: 'flex',
                  alignItems: 'center'
                }}>
                  <WikiButton
                    isSelected={rightSidebarExpanded}
                    onClick={toggleSidebar}
                  />
                </SidebarColumn>}
              </Row>
            </div>
            <Row>
              <GridColumn className='table-wrap'>
                <StatefulTableViewer
                  virtualized
                  className='table'
                  tableData={this.filteredTableData()}
                  resetScrollTop={resetScrollTop}
                  style={{width:'100%'}}
                  {...passAlongProps}
                />
              </GridColumn>
              {rightSidebarExpanded && rightSidebar && <SidebarColumn style={{ position: 'relative' }}>
                <BrowseTableResizer anchorElementGetter={this.getMainContainer} />
                {rightSidebar}
              </SidebarColumn>}
            </Row>
          </div>
        </div>
      </div>
    );
  }
}

const styles = { // todo: RTL support
  heading: {
    flexShrink: 1,
    minWidth: 0,
    direction: 'rtl' // use RTL mode as a hack to make truncation start at the left...
  }
};
const LRE = '\u202A'; // ... but make sure the text is treated as LTR by the text engine (e.g. render '@dremio', not 'dremio@')
// note: wrapping in <div> with direction:ltr doesn't produce "..."

