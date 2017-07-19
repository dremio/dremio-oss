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
import { PropTypes, Component } from 'react';

import AmazonS3 from './AmazonS3';

export default class SelectedDb extends Component {
  static propTypes = {
    type: PropTypes.string.isRequired,
    history: PropTypes.object.isRequired
  }

  constructor(props) {
    super(props);
    this.getDbForm = this.getDbForm.bind(this);
  }

  getDbForm() {
    const components = {
      'amazons3': <AmazonS3 history={this.props.history}/>
    };
    const { type } = this.props;
    return components[type] || components.amazons3;
  }

  render() {
    return (
      <div className='selected-db'>
        {this.getDbForm()}
      </div>
    );
  }
}
