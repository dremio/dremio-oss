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
import React, { Component } from 'react';
import Immutable  from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import { PALE_BLUE } from 'uiTheme/radium/colors';
import FilterSelectMenu from 'components/Fields/FilterSelectMenu';
import SelectMenu from 'components/Fields/SelectMenu';
import FontIcon from 'components/Icon/FontIcon';

import ContainsText from './ContainsText';
import * as IntervalTypes from './StartTimeSelect/IntervalTypes';
import StartTimeSelect from './StartTimeSelect/StartTimeSelect';
import './JobsFilters.less';

const itemsForStateFilter = [ // todo: `la` loc not building correctly here
  {id: 'RUNNING', label: ('Running'), icon: 'Loader'},
  {id: 'COMPLETED', label: ('Completed'), icon: 'OKSolid'},
  {id: 'FAILED', label: ('Failed'), icon: 'ErrorSolid'},
  {id: 'CANCELED', label: ('Canceled'), icon: 'Canceled' }
];

const itemsForQueryTypeFilter = [ // todo: `la` loc not building correctly here
  {id: 'UI', label: ('UI'), default: true},
  {id: 'EXTERNAL', label: ('External Tools'), default: true},
  {id: 'ACCELERATION', label: ('Accelerator'), default: false},
  {id: 'INTERNAL', label: ('Internal'), default: false},
  {id: 'DOWNLOAD', label: ('Downloads'), default: false}
];

const itemsForMoreFilter = [ // todo: `la` loc not building correctly here
  {id: 'spc', label: ('Space'), filterName: 'spaces'}
  //{id: 'usr', label: 'User', filterName: 'users'} //TODO uncomment when user filter will be needed
];

const sortItems = Immutable.fromJS([ // todo: `la` loc not building correctly here
  {id: 'usr', label: ('User')},
  {id: 'st', label: ('Start Time')},
  {id: 'dur', label: ('Duration')},
  {id: 'et', label: ('End Time')}
]);

@injectIntl
@Radium
export default class JobsFilters extends Component {
  static contextTypes = {
    loggedInUser: PropTypes.object.isRequired
  };

  static propTypes = {
    queryState: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,
    dataWithItemsForFilters: PropTypes.object,
    loadItemsForFilter: PropTypes.func.isRequired,
    onUpdateQueryState: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  static defaultProps = {
    queryState: Immutable.fromJS({filters: {}}),
    dataWithItemsForFilters: Immutable.Map()
  };

  constructor(props) {
    super(props);
    this.handleStartTimeChange = this.handleStartTimeChange.bind(this);
    this.addInfoToFilter = this.addInfoToFilter.bind(this);
    this.removeInfoFromFilter = this.removeInfoFromFilter.bind(this);
    this.toggleSortDirection = this.toggleSortDirection.bind(this);
    this.changeSortItem = this.changeSortItem.bind(this);
    this.state = {
      customFilters: Immutable.OrderedSet()
    };
    this.handleEnterText = this.handleEnterText.bind(this);
  }

  componentWillMount() {
    this.props.loadItemsForFilter('spaces');
    if (this.context.loggedInUser.admin) {
      this.props.loadItemsForFilter('users');
    }
  }

  getMoreFilterItems() {
    return itemsForMoreFilter.filter(item => {
      const spaces = this.props.dataWithItemsForFilters.get('spaces');
      //const users = this.props.dataWithItemsForFilters.get('users');
      if (item.id === 'spc' && (!spaces || !spaces.length)) {
        return false;
      }
      //TODO uncomment when user filter will be needed
      /*if (item.id === 'usr' && (!users || !users.length)) {
        return false;
      }*/
      return true;
    });
  }

  getAllFilters() {
    const { queryState, intl } = this.props;
    const { loggedInUser } = this.context;
    const startTime = queryState.getIn(['filters', 'st', 0]) || 0;
    const endTime = queryState.getIn(['filters', 'st', 1]) || 0;
    const selectedJst = queryState.getIn(['filters', 'jst']);
    //const selectedSpc = queryState.getIn(['filters', 'spc']);
    const selectedQt = queryState.getIn(['filters', 'qt']);
    const selectedUsr = queryState.getIn(['filters', 'usr']);
    return [
      {
        value: 'st',
        default: true,
        isVisible: true,
        node: (
          <StartTimeSelect
            selectedToTop={false}
            onChange={this.handleStartTimeChange}
            id='startTimeFilter'
            defaultType={startTime ? IntervalTypes.CUSTOM_INTERVAL : IntervalTypes.ALL_TIME_INTERVAL}
            startTime={startTime}
            endTime={endTime}
          />
        )
      },
      {
        value: 'jst',
        default: true,
        isVisible: true,
        node: (
          <FilterSelectMenu
            selectedToTop={false}
            noSearch
            onItemSelect={this.addInfoToFilter.bind(this, 'jst')}
            onItemUnselect={this.removeInfoFromFilter.bind(this, 'jst')}
            selectedValues={selectedJst}
            items={itemsForStateFilter}
            label={intl.formatMessage({ id: 'Common.Status' })}
            name='jst'
          />
        )
      },
      {
        value: 'qt',
        default: true,
        isVisible: true,
        node: (
          <FilterSelectMenu
            selectedToTop={false}
            noSearch
            onItemSelect={this.addInfoToFilter.bind(this, 'qt')}
            onItemUnselect={this.removeInfoFromFilter.bind(this, 'qt')}
            selectedValues={selectedQt}
            items={itemsForQueryTypeFilter}
            label={intl.formatMessage({ id: 'Common.Type' })}
            name='qt'
          />
        )
      },
      {
        value: 'usr',
        default: true,
        isVisible: this.props.dataWithItemsForFilters.get('users') &&
          this.props.dataWithItemsForFilters.get('users').length,
        node: (
          <FilterSelectMenu
            selectedToTop
            searchPlaceholder='Search users'
            onItemSelect={this.addInfoToFilter.bind(this, 'usr')}
            onItemUnselect={this.removeInfoFromFilter.bind(this, 'usr')}
            selectedValues={selectedUsr}
            items={this.props.dataWithItemsForFilters.get('users')}
            loadItemsForFilter={this.props.loadItemsForFilter.bind(this, 'users')}
            label={intl.formatMessage({ id: 'Common.User' })}
            name='usr'
          />
        )
      }
/*  Removed because this doesn't work at all (DX-5317)
      {
        value: 'spc',
        default: true,
        isVisible: this.props.dataWithItemsForFilters.get('spaces') &&
          this.props.dataWithItemsForFilters.get('spaces').length,
        node: (
          <FilterSelectMenu
            selectedToTop
            searchPlaceholder='Search spaces'
            onItemSelect={this.addInfoToFilter.bind(this, 'spc')}
            onItemUnselect={this.removeInfoFromFilter.bind(this, 'spc')}
            selectedValues={selectedSpc}
            items={this.props.dataWithItemsForFilters.get('spaces')}
            loadItemsForFilter={this.props.loadItemsForFilter.bind(this, 'spaces')}
            label={intl.formatMessage({ id: 'Space.Space' })}
            name='spc'
          />
        )
      }
*/
    ].filter(filter => filter.value !== 'usr' && !loggedInUser.admin || loggedInUser.admin);
  }

  handleStartTimeChange(type, rangeObj) {
    const {queryState} = this.props;
    const range = rangeObj && rangeObj.toJS && rangeObj.toJS();
    const fromDate = range && range[0];
    const toDate = range && range[1];
    const fromDateTimestamp = fromDate && fromDate.toDate().getTime();
    const toDateTimestamp = toDate && toDate.toDate().getTime();

    this.props.onUpdateQueryState(queryState.setIn(['filters', 'st'], [fromDateTimestamp, toDateTimestamp]));
  }

  handleEnterText(text) {
    const {queryState} = this.props;
    const newState = text ?
      queryState.setIn(['filters', 'contains'], Immutable.List([text]))
      : queryState.deleteIn(['filters', 'contains']);
    this.props.onUpdateQueryState(newState);
  }

  addInfoToFilter(type, value) {
    const { queryState } = this.props;
    const values = queryState.getIn(['filters', type]) || Immutable.List();

    if (!values.includes(value)) {
      this.props.onUpdateQueryState(queryState.setIn(['filters', type], values.push(value)) );
    }
  }

  removeInfoFromFilter(type, value) {
    const { queryState } = this.props;
    const values = queryState.getIn(['filters', type]) || Immutable.List();

    const index = values.indexOf(value);
    if (index !== -1) {
      const newState = values.size > 1 ?
        queryState.setIn(['filters', type], values.remove(index))
        : queryState.deleteIn(['filters', type]);
      this.props.onUpdateQueryState(newState);
    }
  }

  toggleSortDirection() {
    const {queryState} = this.props;
    const oldDirection = queryState.get('order');
    const newDirection = oldDirection === 'ASCENDING' ? 'DESCENDING' : 'ASCENDING';
    this.props.onUpdateQueryState(queryState.set('order', newDirection));
  }

  changeSortItem(id) {
    const {queryState} = this.props;
    const order = queryState.get('order');
    this.props.onUpdateQueryState(queryState.set('sort', id).set('order', order || 'DESCENDING'));
  }

  renderAllFilters(items, isCustom) {
    return items.filter(item => item.isVisible).map(filter => (
      <div style={styles.filterBlock} key={filter.value}>
        <div>{React.cloneElement(filter.node, {isCustom})}</div>
        <div style={styles.divider}/>
      </div>
      )
    );
  }

  renderDefaultFilters() {
    return this.renderAllFilters(this.getAllFilters().filter(filter => filter.default));
  }

  renderSortLabel() {
    const sortId = this.props.queryState.get('sort');
    const selectedItem = sortItems.find(item => {
      return item.get('id') === sortId;
    });
    const label = selectedItem ? selectedItem.get('label') : '';
    return (
      <div
        data-qa='order-filter'
        style={[styles.filterBlock, {cursor: 'pointer'}]}
        onClick={this.toggleSortDirection}
      >
        <label style={{cursor: 'pointer'}}>
          {this.props.intl.formatMessage({ id: 'Job.OrderBy' }, { label })}
          {this.renderSortDirectionIcon()}
        </label>
        <div style={styles.sortDivider}/>
      </div>
    );
  }

  renderSortDirectionIcon() {
    const direction = this.props.queryState.get('order');
    if (direction) {
      const type = direction === 'DESCENDING'
        ? 'fa-caret-down'
        : 'fa-caret-up';
      return <FontIcon type={type} theme={styles.IconTheme}/>;
    }
  }

  renderFilters() {
    const {queryState} = this.props;
    return (
      <div style={[styles.base, styles.filtersHeader]}>
        {this.renderDefaultFilters()}
        <ContainsText
          defaultValue={queryState.getIn(['filters', 'contains', 0])}
          id='containsText'
          onEnterText={this.handleEnterText}
        />
        <div style={styles.order}>
          {this.renderSortLabel()}
          {/* todo: SelectMenu is deprecated */}
          <SelectMenu
            hideSelectedLabel
            name='sort'
            selectedItem={queryState.get('sort')}
            items={sortItems}
            onItemSelect={this.changeSortItem}
          />
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className='filters-header' style={styles.base}>
        {this.renderFilters()}
      </div>);
  }
}

const styles = {
  IconTheme: {
    Container: {
      float: 'right',
      margin: '0'
    }
  },
  base: {
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10,
    backgroundColor: PALE_BLUE
  },
  orderBy: {
    margin: '0 0 0 auto'
  },
  filtersHeader: {
    paddingLeft: 0
  },
  divider: {
    background: 'rgba(0,0,0,0.10)',
    height: 16,
    width: 1,
    marginLeft: 10,
    marginRight: 10
  },
  sortDivider: {
    background: 'rgba(0,0,0,0.10)',
    height: 16,
    width: 1,
    marginLeft: 10
  },
  filterBlock: {
    display: 'flex',
    alignItems: 'center'
  },
  order: {
    display: 'flex',
    marginLeft: 'auto'
  }
};
