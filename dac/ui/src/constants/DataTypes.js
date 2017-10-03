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
import moment from 'moment';

export const TEXT = 'TEXT';
export const BINARY = 'BINARY';
export const BOOLEAN = 'BOOLEAN';
export const FLOAT = 'FLOAT';
export const DECIMAL = 'DECIMAL';
export const INTEGER = 'INTEGER';
export const BIGINT = 'BIGINT'; // can MAYBE remove BIGINT with DX-5403 Acceleration UI missing type icons due to incorrect types from BE
export const MIXED = 'MIXED';
export const DATE = 'DATE';
export const TIME = 'TIME';
export const DATETIME = 'DATETIME';
export const LIST = 'LIST';
export const MAP = 'MAP';
export const GEO = 'GEO';
export const OTHER = 'OTHER';
export const JSONTYPE = 'JSON'; // todo: no icon - is this actually a thing?
export const ANY = 'ANY';

export const typeToIconType = {
  [TEXT]: 'TypeText',
  [BINARY]: 'TypeBinary',
  [BOOLEAN]: 'TypeBoolean',
  [FLOAT]: 'TypeFloat',
  [DECIMAL]: 'TypeDecimal',
  [INTEGER]: 'TypeInteger',
  [BIGINT]: 'TypeInteger', // can MAYBE remove BIGINT with DX-5403 Acceleration UI missing type icons due to incorrect types from BE
  [MIXED]: 'TypeMixed',
  [DATE]: 'Date',
  [TIME]: 'Time',
  [DATETIME]: 'TypeDateTime',
  [LIST]: 'TypeList',
  [MAP]: 'TypeMap',
  [GEO]: 'TypeGeo',
  [OTHER]: 'TypeOther',
  [ANY]: 'TypeOther'
};

export const dateTypeToFormat = {
  [DATE]: 'YYYY-MM-DD',
  [TIME]: 'HH:mm:ss',
  [DATETIME]: 'YYYY-MM-DD HH:mm:ss'
};

const typesToTransformType = {
  [TEXT]: {
    [DATE]: 'ConvertTextToDate',
    [DATETIME]: 'ConvertTextToDate',
    [TIME]: 'ConvertTextToDate',
    [INTEGER]: 'ConvertToTypeIfPossible',
    [FLOAT]: 'ConvertToTypeIfPossible',
    [JSONTYPE]: 'ConvertFromJSON'
  },
  [BINARY] : {
    [INTEGER]: 'ConvertToTypeIfPossible',
    [FLOAT]: 'ConvertToTypeIfPossible',
    [JSONTYPE]: 'ConvertFromJSON',
    [DATE]: 'ConvertBinaryToDate',
    [TIME]: 'ConvertBinaryToDate',
    [DATETIME]: 'ConvertBinaryToDate'
  },
  [FLOAT]: {
    [INTEGER]: 'ConvertFloatToInteger',
    [DATE]: 'ConvertNumberToDate',
    [TIME]: 'ConvertNumberToDate',
    [DATETIME]: 'ConvertNumberToDate'
  },
  [DECIMAL]: {
    [INTEGER]: 'ConvertFloatToInteger',
    [DATE]: 'ConvertNumberToDate',
    [TIME]: 'ConvertNumberToDate',
    [DATETIME]: 'ConvertNumberToDate'
  },
  [INTEGER]: {
    [DATE]: 'ConvertNumberToDate',
    [TIME]: 'ConvertNumberToDate',
    [DATETIME]: 'ConvertNumberToDate'
  },
  [DATE] : {
    [TEXT]: 'ConvertDateToText',
    [INTEGER]: 'ConvertDateToNumber',
    [FLOAT]: 'ConvertDateToNumber'
  },
  [TIME] : {
    [TEXT]: 'ConvertDateToText',
    [INTEGER]: 'ConvertDateToNumber',
    [FLOAT]: 'ConvertDateToNumber'
  },
  [DATETIME] : {
    [TEXT]: 'ConvertDateToText',
    [INTEGER]: 'ConvertDateToNumber',
    [FLOAT]: 'ConvertDateToNumber'
  },
  [LIST] : {
    //list is special cased
  },
  [MAP] : {
    [TEXT] : 'ConvertToJSON'
  }
};

const dateTypes = [DATE, DATETIME, TIME];
export const isDateType = (type) => dateTypes.indexOf(type) !== -1;
export const getDefaultValue = (columnType, value = 0) => {
  if (isDateType(columnType)) {
    return moment.utc(Number(value)).format(dateTypeToFormat[columnType]);
  }

  return value;
};

export const convertToUnix = (value, columnType) => {
  if (!value) {
    return null;
  }
  const valMoment = moment.utc(value, dateTypeToFormat[columnType]);
  return (columnType === TIME ? valMoment.date(1).month(0).year(1970) : valMoment).valueOf();
};

export function getTransformType(values, fromDataType, toDataType) {
  if (typesToTransformType[fromDataType] && typesToTransformType[fromDataType][toDataType]) {
    return typesToTransformType[fromDataType][toDataType];
  }

  if (fromDataType === LIST && toDataType === TEXT) {
    if (values.format === 'json') {
      return 'ConvertToJSON';
    }
    return 'ConvertListToText';
  }

  return 'SimpleConvertToType';
}

export function parseTextToDataType(cellText, dataType) {
  switch (dataType) {
  case TIME:
  case DATETIME:
  case DATE:
    return convertToUnix(cellText, dataType);
  case INTEGER:
  case FLOAT:
  case DECIMAL:
  case BIGINT: // can MAYBE remove BIGINT with DX-5403 Acceleration UI missing type icons due to incorrect types from BE
    return Number(cellText);
  case BOOLEAN:
    return cellText === 'true';
  default:
    return cellText;
  }
}
