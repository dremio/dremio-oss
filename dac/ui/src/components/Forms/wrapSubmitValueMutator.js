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


/**
 * A HOC that wraps redux form and overrides handleSubmit to call {@see mutateSubmitValues} before passing
 * values to original handleSubmit function, which is provided by redux form *
 * @export
 * @param {func} mutateSubmitValues
 * @param {*} Form
 * @returns
 */
export default function wrapSubmitValueMutator(mutateSubmitValues, Form) {
  const Wrapper = class extends Component {
    static propTypes = {
      handleSubmit: PropTypes.func.isRequired // from redux-form
    }

    handleSubmit = (onSubmit) => {
      return this.props.handleSubmit((values) => {
        mutateSubmitValues(values, this.props);
        return onSubmit(values);
      });
    }

    render() {
      return <Form {...this.props} handleSubmit={this.handleSubmit} />;
    }
  };

  return Wrapper;
}
