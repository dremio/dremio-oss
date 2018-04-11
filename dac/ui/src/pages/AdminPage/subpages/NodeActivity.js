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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { loadNodeCredentials } from 'actions/admin';
import NodeActivityView, { VIEW_ID as NODE_ACTIVITY_VIEW_ID } from './NodeActivityView';

import './NodeActivity.less';   // TODO to Vasyl, need to use Radium
const MINUTE = 60000;

@pureRender
class NodeActivity extends Component {
  static propTypes = {
    loadNodeCredentials: PropTypes.func,
    sourceNodesList: PropTypes.object
  }

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    this.props.loadNodeCredentials(NODE_ACTIVITY_VIEW_ID);
  }

  componentWillReceiveProps() {
    clearTimeout(this.updateNode);
    this.updateNode = setTimeout(() => (this.props.loadNodeCredentials(NODE_ACTIVITY_VIEW_ID)), MINUTE);
  }

  componentWillUnmount() {
    clearTimeout(this.updateNode);
  }

  render() {
    return (
      <NodeActivityView sourceNodesList={this.props.sourceNodesList}/>
    );
  }
}

function mapStateToProps(state) {
  return {
    sourceNodesList: state.admin.get('sourceNodesList')
  };
}

export default connect(mapStateToProps, {
  loadNodeCredentials
})(NodeActivity);
