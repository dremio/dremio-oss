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
const path = require('path');
//const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const config = require('./webpack.config.js');

// reference: http://stackoverflow.com/questions/32385219/mocha-tests-dont-run-with-webpack-and-mocha-loader/32386750#32386750

const resolveModules = config.resolve.modules;
resolveModules.push(path.resolve(__dirname, 'test'));

module.exports = {
  entry: './test/browserIndex.js',

  output: {
    filename: 'test.build.js',
    path: '/test',
    publicPath: '/'
  },

  module: {
    noParse: [
      /node_modules\/sinon\//
    ],
    loaders: config.module.loaders
  },

  plugins: [
    new ExtractTextPlugin('style.css', { allChunks: true }),
    new HtmlWebpackPlugin({ template: path.join(__dirname, '/test/test.html') })
  ],

  externals: {
    'jsdom': 'window',
    'cheerio': 'window',
    'global': 'window',
    'react/lib/ExecutionEnvironment': true,
    'react/lib/ReactContext': 'window',
    'react/addons': true
  },

  resolveLoader: config.resolveLoader,

  resolve: {
    modules: resolveModules,
    alias: Object.assign({}, config.resolve.alias, { // eslint-disable-line no-restricted-properties
      sinon: path.resolve('./node_modules/sinon/pkg/sinon.js')
    })
  }
};
