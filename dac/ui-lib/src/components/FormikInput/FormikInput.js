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
import React from 'react';
import PropTypes from 'prop-types';

import Input from '../Input';

const FormikInput = (props) => {

  const {
    field,
    ...otherProps
  } = props;

  return (
    <Input
      {...field}
      {...otherProps}
    />
  );
};

FormikInput.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.string,
  labelStyle: PropTypes.object,
  classes: PropTypes.shape({
    root: PropTypes.string,
    input: PropTypes.string
  }),
  field: PropTypes.object.isRequired,
  form: PropTypes.object.isRequired,
  hideError: PropTypes.bool,
  prefix: PropTypes.string
};

FormikInput.defaultProps = {
  disabled: false,
  label: null,
  labelStyle: {},
  classes: {},
  hideError: false
};

export default FormikInput;
