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

const contextKey = '_dremioLocation';

export const getContext = location => ({ // export of this key is needed for tests
  [contextKey]: location
});

export class LocationProvider extends Component {
  static propTypes = {
    location: PropTypes.object.isRequired,
    children: PropTypes.object
  }

  static childContextTypes = {
    [contextKey]: PropTypes.object
  }

  getChildContext = () => getContext(this.props.location);

  render() {
    return this.props.children;
  }
}

function getDisplayName(WrappedComponent) {
  return WrappedComponent.displayName || WrappedComponent.name || 'Component';
}

export const withLocation = (Comp) => {
  const witLocationComp = class extends Component {

    static contextTypes = {
      [contextKey]: PropTypes.object.isRequired
    };

    render() {
      const {
        [contextKey]: location
      } = this.context;
      return (<Comp {...this.props} location={location} />);
    }
  };
  witLocationComp.displayName = `withLocation(${getDisplayName(Comp)})`;


  return witLocationComp;
};
