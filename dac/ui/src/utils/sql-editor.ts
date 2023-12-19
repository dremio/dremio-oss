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

export const SQL_LIGHT_THEME = "vs";
export const SQL_DARK_THEME = "vs-dark";

const SQL_EDITOR_THEMES = {
  [SQL_LIGHT_THEME]: {
    "": "313131", // general text
    "operator.sql": "313131",
    delimiter: "313131",
    "string.sql": "077D82",
    functions: "BE4C20",
    comment: "906200",
    keywords: "124DB4",
    keyword: "124DB4",
    number: "5117F5",
    datatypes: "C700B5",
    nullValue: "CE0065",
  },
  [SQL_DARK_THEME]: {
    "": "EBEBEB", // general text
    "operator.sql": "EBEBEB",
    delimiter: "EBEBEB",
    "string.sql": "7FDBDF",
    functions: "FF9068",
    comment: "E3D27D",
    keywords: "A4D4FF",
    keyword: "A4D4FF",
    number: "A3A8D7",
    datatypes: "FF96FF",
    nullValue: "FFAEDC",
  },
};

export const getSQLEditorThemeRules = (theme: "vs" | "vs-dark") => {
  return [
    ...Object.entries(SQL_EDITOR_THEMES[theme]).map(([key, val]: any) => ({
      token: key,
      foreground: val,
    })),
  ];
};

// The regex rule in tokenProvider that reads default/custom tokens, and map tokens to colors
export const TOKEN_NOTATION_REGEX = /[\w@#$]+/;

// Defining custom tokens in tokenProvider.tokenizer.root, in the /[\w@#$]+/ rule
export const getSQLTokenNotationMap = () => ({
  cases: {
    // custom
    "@datatypes": "datatypes",
    "@functions": "functions",
    "@keywords": "keywords",
    "@nullValue": "nullValue",
    // default
    "@builtinFunctions": "predefined",
    "@builtinVariables": "predefined",
    "@operators": "operator",
  },
});
