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

console.info(`'${__filename}' config is loaded`);
module.exports = {
  'presets': [
    ['@babel/preset-env',
      {
        useBuiltIns: 'entry',
        corejs: 2
      }
    ],
    '@babel/preset-react',
    '@babel/preset-typescript'
  ],
  'plugins': [
    '@babel/plugin-proposal-export-default-from',
    // plugin-proposal-decorators must be before @babel/plugin-proposal-class-properties
    // see https://babeljs.io/docs/en/babel-plugin-proposal-decorators#note-compatibility-with-babel-plugin-proposal-class-properties
    ['@babel/plugin-proposal-decorators', { 'legacy': true }],
    ['@babel/plugin-proposal-class-properties', { 'loose' : true }],
    '@babel/plugin-transform-runtime',
    '@babel/plugin-transform-modules-commonjs'
  ],
  'env': {
    'production': {
      'plugins': [
        '@babel/plugin-transform-react-constant-elements',
        '@babel/plugin-transform-react-inline-elements'
      ]
    },
    'development': {
    }
  }
};
