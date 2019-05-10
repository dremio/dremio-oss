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
import {
  BIGINT,
  BINARY,
  BOOLEAN,
  DATE,
  DATETIME,
  DECIMAL,
  FLOAT,
  INTEGER,
  LIST,
  MAP,
  MIXED,
  TEXT,
  TIME,
  DOUBLE,
  TIMESTAMP,
  VARCHAR
} from 'constants/DataTypes';

// todo: why is this missing some items from constants/DataTypes.js?
export const ALL_TYPES = [
  TEXT,
  BINARY,
  INTEGER,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME,
  LIST,
  MAP,
  BOOLEAN,
  MIXED,
  BIGINT,
  DOUBLE,
  TIMESTAMP,
  VARCHAR
];

export const KEEP_ONLY_TYPES = [
  TEXT,
  INTEGER,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME,
  BOOLEAN
];

export const SORTABLE_TYPES = [
  TEXT,
  BINARY,
  INTEGER,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME,
  BOOLEAN,
  MIXED
];

export const CONVERTIBLE_TYPES = [
  TEXT,
  // BINARY, disabled pending DX-5159
  INTEGER,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME,
  LIST,
  MAP,
  BOOLEAN
];


export const REPLACEABLE_TYPES = [
  INTEGER,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME,
  BOOLEAN
];

export const NOT_LIST_AND_MAP_TYPES = ALL_TYPES.filter((type) => [MAP, LIST].indexOf(type) === -1);

export const NUMBER_TYPES = [
  INTEGER,
  FLOAT,
  DECIMAL
];

export const BIN_TYPES = [
  FLOAT,
  DECIMAL
];

export const TO_BINARY_TYPES = [
  TEXT
];

export const TO_INTEGER_TYPES = [
  TEXT,
  FLOAT,
  DECIMAL,
  DATE,
  TIME,
  DATETIME
  // BOOLEAN see DX-7055
];

export const TO_FLOAT_TYPES = [
  TEXT,
  INTEGER,
  DATE,
  TIME,
  DATETIME
];

export const AUTO_TYPES = [
  TEXT,
  BINARY
];

export const TO_DATE_TYPES = [
  INTEGER,
  FLOAT,
  DECIMAL,
  TEXT,
  DATE,
  DATETIME
];

export const TO_TIME_TYPES = [
  INTEGER,
  FLOAT,
  DECIMAL,
  TEXT,
  DATETIME
];

export const DATE_TYPES = [
  DATETIME,
  TIME,
  DATE
];

// changes to this require changes to user-facing text in AccelerationUpdatesForm
export const INCREMENTAL_TYPES = [
  BIGINT,
  INTEGER,
  DATE,
  TIMESTAMP,
  FLOAT,
  DOUBLE,
  DECIMAL,
  VARCHAR
];
