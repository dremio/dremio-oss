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

import Transform from 'pages/ExplorePage/components/Transform/Transform';

@PureRender
@Radium
class TransformContent extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    changeFormType: PropTypes.func.isRequired,
    submit: PropTypes.func.isRequired,
    cancel: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div style={[style.base]} className='transform-content'>
        <Transform
          dataset={this.props.dataset}
          submit={this.props.submit}
          cancel={this.props.cancel}
          changeFormType={this.props.changeFormType}
          handleModelChange={this.handleModelChange}
          location={this.props.location}/>
      </div>
    );
  }
}

const style = {
  'base': {
    'height': '100%',
    'position': 'relative',
    backgroundColor: '#F5FCFF'
  }
};

export default TransformContent;
