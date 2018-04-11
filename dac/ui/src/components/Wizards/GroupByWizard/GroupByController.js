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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import GroupBy from 'pages/ExplorePage/components/GroupBy/GroupBy';

@pureRender
@Radium
export default class GroupByController extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    columns: PropTypes.instanceOf(Immutable.List),
    changeFormType: PropTypes.func.isRequired,
    submit: PropTypes.func,
    cancel: PropTypes.func
  }

  static contextTypes = {
    location: PropTypes.object
  }

  render() {
    return (
      <div className='group-by'>
        <GroupBy
          dataset={this.props.dataset}
          columns={this.props.columns}
          location={this.context.location}
          submit={this.props.submit}
          onCancel={this.props.cancel}
          changeFormType={this.props.changeFormType}
        />
      </div>
    );
  }
}
