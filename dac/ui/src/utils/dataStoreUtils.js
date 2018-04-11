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
import config from 'utils/config';

// todo: loc - all this one-off custom loc stuff needs tearing out and replacing

const languageHash = {
  'en': 'dataEN'
};

let languages = {};

const defaultLanguage = config.useRunTimeLanguage
  ? languageHash[window.language] || 'dataEN'
  : languageHash[config.language] || 'dataEN';

if (config.useRunTimeLanguage) {
  languages = require('./dataAll');
} else {
  languages[defaultLanguage] = {
    dropdownItemsForTransformControls:
      require('./' + defaultLanguage + '/explore/dropdownItemsForTransformControls.json'),
    datasetModel: require('./' + defaultLanguage + '/home/datasetModel.json'),
    radioButtonsForAddResource: require('./' + defaultLanguage + '/home/radioButtonsForAddResource.json'),
    exploreInfoBlock: require('./' + defaultLanguage + '/explore/exploreInfoBlock.json'),
    btnsHeaderForGrid: require('./' + defaultLanguage + '/explore/btnsHeaderForGrid.json'),
    btnsHeaderForPhysicalDataset: require('./' + defaultLanguage + '/explore/btnsHeaderForPhysicalDataset.json'),
    sectionsForSources: require('./' + defaultLanguage + '/home/sectionsForSources.json'),
    subtypeForTransformTab: require('./' + defaultLanguage + '/explore/subtypeForTransformTab.json'),
    dataForAccelerationFrequency: require('./' + defaultLanguage + '/home/dataForAccelerationFrequency.json'),
    dataTypeForConverter: require('./' + defaultLanguage + '/explore/dataTypeForConverter.json')
  };
}

class DataStoreUtils {
  contains(arr, elem) {
    return arr.indexOf(elem) !== -1;
  }

  getCurrentLanguage() {
    return languageHash[config.language || window.language] || 'dataEN';
  }

  getHomeHeaderData() {
    return languages[this.getCurrentLanguage()].homeHeaderData;
  }

  getDropdownItemsForTransformControls() {
    return languages[this.getCurrentLanguage()].dropdownItemsForTransformControls;
  }

  getItemsForDataset() {
    return languages[this.getCurrentLanguage()].datasetModel;
  }

  getRadioButtonsForAddResource() {
    return languages[this.getCurrentLanguage()].radioButtonsForAddResource;
  }

  getItemsForExploreInfo() {
    return languages[this.getCurrentLanguage()].exploreInfoBlock;
  }

  getNamesForBtnsHeaderForGrid() {
    return languages[this.getCurrentLanguage()].btnsHeaderForGrid;
  }

  getNamesForBtnsHeaderForGridPhysical() {
    return languages[this.getCurrentLanguage()].btnsHeaderForPhysicalDataset;
  }

  getSectionsForSources() {
    return languages[this.getCurrentLanguage()].sectionsForSources;
  }

  getSubtypeForTransformTab() {
    return languages[this.getCurrentLanguage()].subtypeForTransformTab;
  }

  getDataForAccelerationFrequency() {
    return languages[this.getCurrentLanguage()].dataForAccelerationFrequency;
  }

  getDataTypeForConverter() {
    return languages[this.getCurrentLanguage()].dataTypeForConverter;
  }
}

const dataStoretUtils = new DataStoreUtils();

export default dataStoretUtils;
