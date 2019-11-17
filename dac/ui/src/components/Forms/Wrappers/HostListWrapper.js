/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import FieldWithError from 'components/Fields/FieldWithError';
import HostList from 'components/Forms/HostList';

import PropTypes from 'prop-types';

export default class HostListWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.array
  };

  render() {
    const {elementConfig, fields, field} = this.props;
    const elementConfigJson = elementConfig.getConfig();
    const defaultPort = elementConfigJson.defaultPort || 9200;
    return (
      <FieldWithError {...field}>
        <HostList fields={fields}
          elementConfig={elementConfigJson}
          defaultPort={defaultPort}
        />
      </FieldWithError>
    );
  }
}
