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
const APP_KEY = 'globalApp';
const emptyApp = {
  home: {
    recentDatasetsToken: '',
    readonlyLinks: '',
    pinnedItems: {
      sources: {},
      spaces: {}
    }
  },
  explore: {
    tasks: [],
    tables: [],
    transform: {
      info: {}
    }
  },
  customData: {}
};

export class LocalStorageUtils {
  constructor() {
    const app = this.getApp();
    if (!app) {
      this._safeSave(APP_KEY, emptyApp);
    }
  }

  getUserData() { // todo: no fake objects: return null if no user
    try {
      const userData = JSON.parse(localStorage.getItem('user'));
      if (typeof userData !== 'object') {
        return {};
      }
      return userData;
    } catch (e) {
      return {};
    }
  }

  setUserData(user) {
    return localStorage.setItem('user', JSON.stringify(user));
  }

  clearUserData() {
    return localStorage.removeItem('user');
  }

  getAuthToken() {
    return this.getUserData() && this.getUserData().token;
  }

  getApp() {
    return this._safeParse(localStorage.getItem(APP_KEY));
  }

  setApp(app) {
    this._safeSave(APP_KEY, app);
  }

  getPinnedItems() {
    const app = this.getApp();
    return app.home.pinnedItems || {};
  }

  updatePinnedItemState(name, state, group) {
    const app = this.getApp();
    if (app.home.pinnedItems && app.home.pinnedItems[group]) {
      app.home.pinnedItems[group][name] = state;
    } else {
      app.home.pinnedItems = {
        ...app.home.pinnedItems,
        [group] : { [name]: state }
      };
    }
    this.setApp(app);
  }

  setCustomValue(key, value) {
    const app = this.getApp();
    app.customData[key] = value;
    this.setApp(app);
  }

  getCustomValue(key) {
    const app = this.getApp();
    return app.customData[key];
  }

  getGridTasks(now) {
    const app = this.getApp();
    const SECONDS = 1000;
    const LIFE_TIME = 100000;
    const filteredTasks = app.explore.tasks.filter((task) => {
      return Math.abs( (now - task.timestamp) / SECONDS ) < LIFE_TIME;
    });
    this.setGridTasks(filteredTasks);
    return filteredTasks;
  }

  setGridTasks(tasks) {
    const app = this.getApp();
    app.explore.tasks = tasks;
    this.setApp(app);
  }

  saveTableColumns(name, version, columns) {
    const app = this.getApp();
    if (!app.explore.tables) {
      app.explore.tables = [];
    }
    let isFound = false;
    app.explore.tables = app.explore.tables.map(table => {
      if (table && table.name === name && table.version === version) {
        table.columns = columns.toJS();
        isFound = true;
      }
      return table;
    });
    if (!isFound) {
      app.explore.tables.push({
        name,
        version,
        columns: columns.toJS()
      });
    }
    this.setApp(app);
  }

  getTableColumns(name, version) {
    const app = this.getApp();
    const curTable = app.explore && app.explore.tables &&
                     app.explore.tables.find(table => table && table.name === name && table.version === version);
    if (!curTable) {
      return null;
    }
    return curTable.columns;
  }

  setDefaultSqlState(sqlState) {
    this._safeSave('sqlState', sqlState);
  }

  getDefaultSqlState() {
    return localStorage.getItem('sqlState') === 'true';
  }

  setDefaultSqlHeight(sqlHeight) {
    this._safeSave('sqlHeight', sqlHeight);
  }

  getDefaultSqlHeight() {
    return +localStorage.getItem('sqlHeight') || 0;
  }

  _safeParse(data) {
    try {
      return JSON.parse(data) || emptyApp;
    } catch (err) {
      console.error('INVALID DATA TO PARSE');
      return emptyApp;
    }
  }

  _safeSave(key, data) {
    localStorage.setItem(key, JSON.stringify(data));
  }

  setTransformValue(values) {
    const { columnType, toType, ...restValues } = values;
    const app = this.getApp();
    const transform = app.transform || {};

    if (!transform[columnType]) {
      transform[columnType] = {};
    }
    transform[columnType][toType] = restValues;
    app.transform = transform;
    this.setApp(app);
  }

  getTransformValue(columnType, toType) {
    const app = this.getApp();
    return app.transform && app.transform[columnType] && app.transform[columnType][toType]
      ? app.transform[columnType][toType]
      : {};
  }
}

// todo: this shouldn't export `undefined` - it should use an in-memory only store
const localStorageUtils = typeof localStorage !== 'undefined' ? new LocalStorageUtils() : undefined;

export default localStorageUtils;
