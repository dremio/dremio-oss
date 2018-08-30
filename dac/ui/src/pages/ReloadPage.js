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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';

// In order to force React to do an internal refresh of whatever the current page is
// we navigate to this "ReloadPage" and then immediately back to where we came from.
// simply `router.replace({pathname: '/reload', state: {to: router.location}});`

export default class ReloadPage extends PureComponent {
  static propTypes = {
    location: PropTypes.object.isRequired
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  }

  componentWillMount() {
    try {
      this.context.router.replace(this.props.location.state.to);
    } catch (e) {
      console.error(e.message);
      this.context.router.replace('/');
    }
  }

  render() {
    return null;
  }
}
