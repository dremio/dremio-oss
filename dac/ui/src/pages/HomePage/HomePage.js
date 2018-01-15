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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';
import { push } from 'react-router-redux';

import { getSortedSpaces, getSortedSources } from 'selectors/resources';

import { loadSourceListData, setSourcePin } from 'actions/resources/sources';
import { loadSpaceListData, setSpacePin } from 'actions/resources/spaces';
import { getViewState } from 'selectors/resources';
import { page } from 'uiTheme/radium/general';

import QlikStateModal from '../ExplorePage/components/modals/QlikStateModal';
import MainHeader from './../../components/MainHeader';
import RecentDatasets from './subpages/RecentDatasets/RecentDatasets';
import LeftTree   from './components/LeftTree';
import './HomePage.less';

class HomePage extends Component {

  static contextTypes = {
    location: PropTypes.object.isRequired,
    routeParams: PropTypes.object.isRequired
  }

  static propTypes = {
    userInfo: PropTypes.object,
    spaces: PropTypes.instanceOf(Immutable.List).isRequired,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    spacesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    loadSpaceListData: PropTypes.func,
    loadSourceListData: PropTypes.func,
    setSourcePin: PropTypes.func,
    push: PropTypes.func,
    setSpacePin: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object
  }

  componentWillMount() {
    this.props.loadSpaceListData();
    this.props.loadSourceListData();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.spacesViewState.get('invalidated')) {
      nextProps.loadSpaceListData();
    }

    if (nextProps.sourcesViewState.get('invalidated')) {
      nextProps.loadSourceListData();
    }
  }

  getCenterContent() {
    return this.context.location.pathname.match('/spaces/recent')
      ? this.getRecentDatasetsContent()
      : '';
  }

  getRecentDatasetsContent() {
    return (
      <div className='page-content'>
        <LeftTree
          spaces={this.props.spaces}
          sources={this.props.sources}
          toggleSpacePin={this.toggleSpacePin}
          toggleSourcePin={this.toggleSourcePin}
          pageType='recent'
          className='col-lg-2 col-md-3'
        />
        <RecentDatasets/>
      </div>
    );
  }

  getUser() {
    const { userInfo } = this.props;
    return userInfo && userInfo.size > 0 ? `@${userInfo.get('homeConfig').get('owner')}` : '';
  }

  toggleSourcePin = (name, state) => {
    this.props.setSourcePin(name, !state);
  }

  toggleSpacePin = (name, state) => {
    this.props.setSpacePin(name, !state);
  }

  // Note were are getting the "ref" to the SearchBar React object.
  render() {
    return (
      <div id='home-page' style={page}>
        <MainHeader />
        <div className='page-content'>
          <LeftTree
            spacesViewState={this.props.spacesViewState}
            sourcesViewState={this.props.sourcesViewState}
            spaces={this.props.spaces}
            sources={this.props.sources}
            routeParams={this.props.routeParams}
            toggleSpacePin={this.toggleSpacePin}
            toggleSourcePin={this.toggleSourcePin}
            push={this.props.push}
            className='col-lg-2 col-md-3'/>
          {this.props.children}
        </div>
        {this.getCenterContent()}
        <QlikStateModal />
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    sources: getSortedSources(state),
    spaces: getSortedSpaces(state),
    userInfo: state.home.config.get('userInfo'),
    spacesViewState: getViewState(state, 'AllSpaces'),
    sourcesViewState: getViewState(state, 'AllSources')
  };
}

export default connect(mapStateToProps, {
  loadSourceListData,
  loadSpaceListData,
  setSpacePin,
  setSourcePin,
  push
})(HomePage);
