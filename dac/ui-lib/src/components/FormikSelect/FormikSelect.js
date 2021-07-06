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

import Select from '../Select';
import MultiSelect from '../MultiSelect';

const FormikSelect = ({ field, multiple, ...otherProps }) => {
  return multiple ? (
    <MultiSelect {...field} {...otherProps} />
  ) : (
    <Select {...field} {...otherProps} />
  );
};

FormikSelect.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    label: PropTypes.string
  }),
  field: PropTypes.object,
  label: PropTypes.string,
  multiple: PropTypes.bool,
  options: PropTypes.arrayOf(PropTypes.shape({
    label: PropTypes.string,
    value: PropTypes.string
  })).isRequired,
  style: PropTypes.object
};

FormikSelect.defaultProps = {
  classes: {},
  field: {},
  label: null,
  multiple: false,
  style: {}
};

export default FormikSelect;
