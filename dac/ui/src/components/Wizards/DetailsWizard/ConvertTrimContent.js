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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import EditTextContent from './EditTextContent';

@PureRender
@Radium
class ConvertTrimContent extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func.isRequired,
    type: PropTypes.string.isRequired,
    columnName: PropTypes.string.isRequired,
    changeFormType: PropTypes.func.isRequired,
    cancel: PropTypes.func
  };

  render() {
    const { type, columnName } = this.props;
    return (
      <div style={[style.base]} className='convert-case-content'>
        <EditTextContent
          dataset={this.props.dataset}
          changeFormType={this.props.changeFormType}
          submit={this.props.submit}
          onCancel={this.props.cancel}
          type={type}
          columnName={columnName}
        />
      </div>
    );
  }
}

const style = {
  'base': {
    'height': '100%',
    'position': 'relative'
  }
};

export default ConvertTrimContent;
