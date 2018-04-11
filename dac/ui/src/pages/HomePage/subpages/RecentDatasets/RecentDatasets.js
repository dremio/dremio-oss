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
import moment from 'moment';
import { connect }   from 'react-redux';
import $ from 'jquery';
import Immutable from 'immutable';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { searchRecentDatasets, loadRecentDatasets,
         removeFromHistory, clearHistory } from 'actions/resources/recent';

import './RecentDatasets.less';
import DayPart from './components/DayPart';

export class RecentDatasets extends Component {

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  static propTypes = {
    loadRecentDatasets: PropTypes.func,
    searchRecentDatasets: PropTypes.func,
    clearHistory: PropTypes.func,
    removeFromHistory: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map),
    recentDatasets: PropTypes.array
  };

  constructor(props) {
    super(props);
    this.renderItem = this.renderItem.bind(this);
    this.mapItems = this.mapItems.bind(this);
    this.getDatabaseLists = this.getDatabaseLists.bind(this);
    this.state = {
      days: this.mapItems([])
    };
  }

  componentWillMount() {
    this.props.loadRecentDatasets();
  }

  componentDidMount() {
    const $scrollContainer = $(window);

    this.scrollCallback = () => {
      if ( Math.abs($scrollContainer.scrollTop() + $scrollContainer.height() - $(document).height()) < 200) {
        if (!this.refs.input || !this.refs.input.value) {
          this.props.loadRecentDatasets(null, true);
        }
      }
    };
    $scrollContainer.on('scroll', this.scrollCallback);
  }

  componentWillReceiveProps(nextProps) {
    if (JSON.stringify(nextProps.recentDatasets) !== JSON.stringify(this.props.recentDatasets)) {
      this.setState({
        days: this.mapItems(nextProps.recentDatasets)
      });
    }
  }

  getDatabaseLists() {
    return Object.keys(this.state.days).map((key) => {
      return (
        <DayPart
          data={this.state.days[key]} name={key} key={key}
          removeFromHistory={this.removeFromHistory.bind(this)}/>
      );
    });
  }

  clearHistory() {
    this.setState({
      days: {}
    });
    this.props.clearHistory();
  }

  componentWillUnMount() {
    const $scrollContainer = $(window);
    $scrollContainer.off('scroll', this.scrollCallback);
  }

  filterDatasets() {
    this.props.searchRecentDatasets(this.refs.input.value);
  }

  mapItems(items) {
    const days = {
      today: [],
      yesterday: []
    };
    for (let i = 0; i < items.length; i++) {
      const dayData = items[i].lastOpened;
      const today = new Date();
      today.setHours(0);
      const distance = Math.floor(moment(dayData).diff(today, 'days', true)) + 1;

      if (distance === 0) {
        days.today.push(items[i]);
      } else if (distance === -1) {
        days.yesterday.push(items[i]);
      } else {
        if (!days[dayData]) {
          days[dayData] = [];
        }
        days[dayData].push(items[i]);
      }
    }
    if (!days.today.length) {
      delete days.today;
    }
    if (!days.yesterday.length) {
      delete days.yesterday;
    }
    return days;
  }

  removeFromHistory(id) {
    function filter(day) {
      return day.id !== id;
    }

    const days = {...this.state.days};
    for (const key in this.state.days) {
      days[key] = days[key].filter(filter);
      if (days[key].length === 0) {
        delete days[key];
      }
    }
    this.setState({days});
    this.props.removeFromHistory(id);
  }

  selectItem(text, item) {
    this.setState({
      days: this.mapItems([item])
    });
  }

  renderItem(item, isHighlighted) {
    const activeStyle = {
      backgroundColor: '#0096FF',
      color: '#fff'
    };
    return (
      <div
        key={item.id}
        style={isHighlighted ? activeStyle : {}}
        className='autocomplete-item'
        id={item.id}>
        {item.name}
      </div>
    );
  }

  render() {
    return (
      <div className='recent-datasets'>
        <div className='recent-header'>
          <span className='name'>Recent Datasets</span>
          <button className='clear' onClick={this.clearHistory.bind(this)}>{la('Clear History')}</button>
        </div>
        <div className='filter'>
          <input onChange={this.filterDatasets.bind(this)} ref='input'/>
          <i className='fa fa-search'></i>
        </div>
        <ViewStateWrapper viewState={this.props.viewState}>
          { this.getDatabaseLists() }
        </ViewStateWrapper>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: state.home.recentDatasets,
    recentDatasets: state.home.recentDatasets.get('datasets').toJS() // TODO replace
  };
}

export default connect(mapStateToProps, {
  loadRecentDatasets,
  searchRecentDatasets,
  clearHistory,
  removeFromHistory
})(RecentDatasets);
