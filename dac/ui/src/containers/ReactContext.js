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

// simulates react 16 context behaviour
import { Component } from 'react';
import PropTypes from 'prop-types';
import uuid from 'uuid';

export const createContext = (defaultValue = null, valuePropTypes) => {
  if (!valuePropTypes) {
    throw new Error('valuePropTypes is required');
  }

  const error = PropTypes.checkPropTypes({ value: valuePropTypes }, { value: defaultValue }, 'defaultValue', 'createContext');

  if (error) {
    throw error;
  }

  const contextKey = '_createContextKey' + uuid.v4(); // generate a unique key for context

  class Provider extends Component {
    static propTypes = {
      value: valuePropTypes,
      children: PropTypes.node
    }

    static defaultProps = {
      value: defaultValue
    };

    static childContextTypes = {
      [contextKey]: valuePropTypes
    }

    getChildContext = () => {
      return {
        [contextKey]: this.props.value || defaultValue
      };
    }

    render() {
      return this.props.children;
    }
  }

  class Consumer extends Component { // must be not PureComponent, as render funciton could produce different result
    static propTypes = {
      children: PropTypes.func // (contextValue) => PropTypes.node
    }

    static contextTypes = {
      [contextKey]: valuePropTypes
    };

    render() {
      const {
        [contextKey]: value
      } = this.context;
      const {
        children
      } = this.props;

      return children(value);
    }
  }

  return {
    Provider,
    Consumer,
    valuePropTypes
  };
};
