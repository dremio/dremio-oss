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

import {TEXT, LIST, DATE, TIME, DATETIME, getTransformType} from 'constants/DataTypes';


export default function mapConvertDataType(form) {
  const { columnName, newFieldName, dropSourceField, columnType, toType, ...data } = form;
  const transformType = getTransformType(data, columnType, toType);

  let fieldTransformation = {
    ...data,
    type: transformType
  };
  // hacky tweaking of params for different transform types

  if (columnType === LIST && toType === TEXT) {
    delete fieldTransformation.format;
  }

  if (fieldTransformation.type === 'SimpleConvertToType') {
    fieldTransformation.dataType = toType;
  } else {
    if ([DATE, TIME, DATETIME].indexOf(toType) !== -1) {
      fieldTransformation.desiredType = toType;
    }

    if ([DATE, TIME, DATETIME].indexOf(columnType) !== -1 && toType !== TEXT) {
      fieldTransformation.convertType = columnType;
      fieldTransformation.desiredType = toType;
    }

    if ([DATE, TIME, DATETIME].indexOf(columnType) !== -1 && toType === TEXT) {
      fieldTransformation.convertType = columnType;
    }
  }

  //remove other arguments for JSON
  if (fieldTransformation.type === 'ConvertToJSON') {
    fieldTransformation = {type: 'ConvertToJSON'};
  }

  return {
    type: 'field',
    sourceColumnName: columnName,
    newColumnName: newFieldName,
    dropSourceColumn: dropSourceField,
    fieldTransformation
  };
}
