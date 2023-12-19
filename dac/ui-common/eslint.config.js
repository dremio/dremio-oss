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
/* eslint-disable */
const merge = require("lodash/merge");

const globals = require("globals");
const jest = require("eslint-plugin-jest");
const js = require("@eslint/js");
const jsxA11y = require("eslint-plugin-jsx-a11y");
const prettier = require("eslint-config-prettier");
const promise = require("eslint-plugin-promise");
const reactHooks = require("eslint-plugin-react-hooks");
const reactRecommended = require("eslint-plugin-react/configs/recommended");
const reactRuntime = require("eslint-plugin-react/configs/jsx-runtime");
const typescriptEslint = require("@typescript-eslint/eslint-plugin");
const typescriptParser = require("@typescript-eslint/parser");

const jestConfig = {
  files: ["src/**/*.test.{ts,tsx}"],
  plugins: { jest },
  rules: {
    ...jest.configs.recommended.rules,
    "jest/no-identical-title": "warn",
    "jest/valid-title": "warn",
  },
  languageOptions: {
    globals: {
      ...globals.jest,
      ...globals.node,
    },
  },
};

const reactConfig = merge(reactRecommended, reactRuntime, {
  languageOptions: {
    globals: {
      global: false,
      JSX: false,
      monaco: false,
      React: false,
    },
  },
  plugins: {
    "jsx-a11y": jsxA11y,
    "react-hooks": reactHooks,
  },
  rules: {
    ...reactHooks.configs.recommended.rules,
    ...jsxA11y.configs.recommended.rules,
    "jsx-a11y/alt-text": "warn",
    "react/display-name": "warn",
    "react/jsx-no-target-blank": "warn",
  },
  settings: {
    react: {
      version: "detect",
    },
  },
});

const tsConfig = {
  files: ["src/**/*.{ts,tsx}"],
  languageOptions: {
    globals: {
      ...globals.browser,
      process: false,
      RequestInfo: false,
      RequestInit: false,
    },
    parser: typescriptParser,
  },
  plugins: {
    "@typescript-eslint": typescriptEslint,
  },
  rules: {
    ...typescriptEslint.configs.recommended.rules,
    "@typescript-eslint/ban-ts-comment": "warn",
    "@typescript-eslint/ban-types": "warn",
    "@typescript-eslint/no-explicit-any": "warn",
    "@typescript-eslint/no-extra-non-null-assertion": "warn",
    "@typescript-eslint/no-namespace": "warn",
    "@typescript-eslint/no-unused-vars": "warn",
  },
};

const promiseConfig = {
  plugins: {
    promise,
  },
  rules: promise.configs.recommended.rules,
};

module.exports = [
  js.configs.recommended,
  jestConfig,
  reactConfig,
  tsConfig,
  promiseConfig,
  prettier,
];
