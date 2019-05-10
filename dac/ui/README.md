# Introduction

## Prerequisites
Please make sure that node v10.4 or higher is installed.

## Setup
- Install dependencies: `npm install`
- Install git hooks: `../../tools/git-hooks/configure.command`

## Commands for macOS & Debian
- Start local server with dev version of project: `npm start`
- Start local server with prod version of project: `npm run startprod`
- Build prod version of project: `npm run build`
- Build minified prod version of project: `npm run buildprod`

## Commands for Windows
- Start local server with dev version of project: `npm start`
- Start local server with prod version of project: `npm run startprodwin`
- Build prod version of project: `npm run buildwin`
- Build minified prod version of project: `npm run buildprodwin`

## Running Unit Tests
- Run the linter and unit tests: `npm test`
- Run specific unit tests: `npm run test:single path/to/test [...path/to/test]`
- Run all tests with --watch: `npm test:watch`
