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
import React, { Component } from 'react';
import PropTypes from 'prop-types';
import hoistNonReactStatic from 'hoist-non-react-statics';

export default function prefixSection(prefix) {
  return function(target) {

    class WrappedSection extends Component {
      static propTypes = {
        fields: PropTypes.object
      };

      static getFields() {
        return target.getFields ? target.getFields().map((field) => `${prefix}.${field}`) : [];
      }

      render() {
        return React.createElement(target, {...this.props, fields: this.props.fields ? this.props.fields[prefix] : {}});
      }
    }

    hoistNonReactStatic(WrappedSection, target);
    WrappedSection.getFields = () => {
      return target.getFields().map((field) => `${prefix}.${field}`);
    };

    WrappedSection.validate = (values) => {
      const result = target.validate ? target.validate(values) : {};
      return Object.keys(result).reduce((errors, key) => ({...errors, [`${prefix}.${key}`]: result[key]}), {});
    };

    return WrappedSection;
  };
}
