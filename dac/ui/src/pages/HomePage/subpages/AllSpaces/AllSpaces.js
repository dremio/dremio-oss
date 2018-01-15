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
import { connect }   from 'react-redux';
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';

import HomePage from 'pages/HomePage/HomePage';
import { loadSpaceListData, setSpacePin } from 'actions/resources/spaces';
import { getSpaces } from 'selectors/resources';

import AllSpacesView from './AllSpacesView.js';

@pureRender
@injectIntl
export class AllSpaces extends Component {
  static propTypes = {
    location: PropTypes.object.isRequired,
    setSpacePin: PropTypes.func,
    loadSpaceListData: PropTypes.func,
    spaces: PropTypes.instanceOf(Immutable.List),
    intl: PropTypes.object.isRequired
  };

  componentWillMount() {
    this.props.loadSpaceListData();
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.location.pathname !== nextProps.location.pathname) {
      this.props.loadSpaceListData();
    }
  }

  toggleActivePin = (name, pinState) => {
    this.props.setSpacePin(name, !pinState);
  }

  render() {
    const { location, spaces, intl } = this.props;
    return (
      <HomePage location={location}>
        <DocumentTitle title={intl.formatMessage({ id: 'Space.AllSpaces' })} />
        <AllSpacesView
          filters={this.filters}
          spaces={spaces}
          toggleActivePin={this.toggleActivePin}
        />
      </HomePage>
    );
  }
}

function mapStateToProps(state) {
  return {
    spaces: getSpaces(state)
  };
}

export default connect(mapStateToProps, {
  loadSpaceListData,
  setSpacePin
})(AllSpaces);
