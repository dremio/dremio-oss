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
import { connect } from 'react-redux';

import { getSortedSources } from 'selectors/resources';
import ApiUtils from 'utils/apiUtils/apiUtils';
import { sourceTypesIncludeS3 } from 'utils/sourceUtils';

import { loadSourceListData } from 'actions/resources/sources';
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
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    loadSourceListData: PropTypes.func,
    push: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object
  }

  state = {
    sourceTypes: []
  };

  componentWillMount() {
    this.props.loadSourceListData();
  }

  componentDidMount() {
    this.setStateWithSourceTypesFromServer();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.sourcesViewState.get('invalidated')) {
      nextProps.loadSourceListData();
    }
  }

  setStateWithSourceTypesFromServer() {
    ApiUtils.fetch('source/type').then(response => {
      response.json().then((result) => {
        this.setState({sourceTypes: result.data});
      });
    }, () => {
      console.error('Failed to load source types.');
    });
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
          sources={this.props.sources}
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

  // Note were are getting the "ref" to the SearchBar React object.
  render() {
    return (
      <div id='home-page' style={page}>
        <MainHeader />
        <div className='page-content'>
          <LeftTree
            sourcesViewState={this.props.sourcesViewState}
            sources={this.props.sources}
            sourceTypesIncludeS3={sourceTypesIncludeS3(this.state.sourceTypes)}
            routeParams={this.props.routeParams}
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
    userInfo: state.home.config.get('userInfo'),
    sourcesViewState: getViewState(state, 'AllSources')
  };
}

export default connect(mapStateToProps, {
  loadSourceListData
})(HomePage);
