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
import { connect }   from 'react-redux';
import Immutable from 'immutable';
import shallowEqual from 'shallowequal';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';

import HomePage from 'pages/HomePage/HomePage';
import { loadSourceListData } from 'actions/resources/sources';
import { getSources } from 'selectors/resources';

import AllSourcesView from './AllSourcesView.js';

@pureRender
@injectIntl
export class AllSources extends Component {

  static propTypes = {
    location: PropTypes.object.isRequired,
    sources: PropTypes.instanceOf(Immutable.List),
    loadSourceListData: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  componentWillReceiveProps(nextProps) {
    if (!shallowEqual(this.props.location.query, nextProps.location.query)) {
      this.props.loadSourceListData();
    }
  }

  render() {
    const { location, sources, intl } = this.props;
    return (
      <HomePage location={location}>
        <DocumentTitle title={intl.formatMessage({ id: 'Source.AllSources' })} />
        <AllSourcesView
          filters={this.filters}
          sources={sources}
        />
      </HomePage>
    );
  }
}

function mapStateToProps(state) {
  return {
    sources: getSources(state)
  };
}

export default connect(mapStateToProps, {
  loadSourceListData
})(AllSources);
