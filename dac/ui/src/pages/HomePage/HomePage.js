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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';

import {getSortedSources} from '@app/selectors/home';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import {sourceTypesIncludeS3} from '@app/utils/sourceUtils';
import {loadSourceListData} from '@app/actions/resources/sources';

import {getViewState} from '@app/selectors/resources';
import {page} from '@app/uiTheme/radium/general';

import QlikStateModal from '../ExplorePage/components/modals/QlikStateModal';
import MainHeader from './../../components/MainHeader';
import LeftTree from './components/LeftTree';
import './HomePage.less';

class HomePage extends Component {

  static contextTypes = {
    location: PropTypes.object.isRequired,
    routeParams: PropTypes.object.isRequired
  };

  static propTypes = {
    userInfo: PropTypes.object,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    loadSourceListData: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object
  };

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
    ApiUtils.fetchJson('source/type', (json) => {
      this.setState({sourceTypes: json.data});
    }, () => {
      console.error(
        la('Failed to load source types. Can not check if S3 is supported. Will not show "Add Sample Source".')
      );
    });
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
