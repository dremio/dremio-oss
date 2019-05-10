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
  parseResourceId,
  splitFullPath,
  getRootEntityType,
  constructFullPath,
  getInitialResourceLocation,
  getUniqueName,
  getRouteParamsFromLocation,
  navigateToExploreDefaultIfNecessary
} from './pathUtils';

describe('pathUtils', () => {

  describe('splitFullPath', () => {
    it('should remove ""', () => {
      expect(splitFullPath('foo."my-folder"')).to.eql(['foo', 'my-folder']);
      expect(splitFullPath('"my-space"."my-folder"')).to.eql(['my-space', 'my-folder']);
      expect(splitFullPath('"my-space".my-folder')).to.eql(['my-space', 'my-folder']);
    });

    it('should ignore . in ""', () => {
      expect(splitFullPath('foo."my.folder"')).to.eql(['foo', 'my.folder']);
      expect(splitFullPath('"my.space"."my.folder"')).to.eql(['my.space', 'my.folder']);
    });

    it('should handle "" as escaped "', () => {
      expect(splitFullPath('foo."""folder"')).to.eql(['foo', '"folder']);
      expect(splitFullPath('foo."my.""folder"')).to.eql(['foo', 'my."folder']);
    });
  });

  describe('parseResourceId', () => {

    it('should return username for home path', () => {
      expect(parseResourceId('/home', 'test_user')).to.eql('"@test_user"');
    });

    it('should return space name', () => {
      expect(parseResourceId('/space/foo', 'test_user')).to.eql('"foo"');
    });

    it('should return source name', () => {
      expect(parseResourceId('/source/foo', 'test_user')).to.eql('"foo"');
    });

    it('should return source name with folders', () => {
      expect(parseResourceId('/source/foo/folder/f1/f2', 'test_user')).to.eql('"foo"."f1"."f2"');
    });

    it('should return username for home path with folder', () => {
      expect(parseResourceId('/home/@test_user/folder/ff', 'test_user')).to.eql('"@test_user"."ff"');
    });

    it('should handle folders with dot in name', () => {
      expect(parseResourceId('/home/@test_user/folder/with.dot', 'test_user')).to.eql('"@test_user"."with.dot"');
    });
  });

  describe('getRootEntityType', () => {
    it('should return home for /', () => {
      expect(getRootEntityType('/')).to.eql('home');
    });

    it('should return space for /space/ds1', () => {
      expect(getRootEntityType('/space/ds1')).to.eql('space');
    });
  });

  describe('constructFullPath', () => {
    it('should return undefined if we have not pathParts', () => {
      expect(constructFullPath()).to.eql(undefined);
    });

    it('should not be quoted pathParts if preventQuoted=true', () => {
      expect(constructFullPath(['Tphc-Sample', 'ds3'], true)).to.eql('Tphc-Sample.ds3');
    });

    it('should be quoted if token of path is SQL word', () => {
      expect(constructFullPath(['TphcSample', 'INSERT'])).to.eql('TphcSample."INSERT"');
      expect(constructFullPath(['TphcSample', 'insert'])).to.eql('TphcSample."insert"');
    });

    it('should be quoted if token of path have not only characters or numbers', () => {
      expect(constructFullPath(['Tphc-Sample'])).to.eql('"Tphc-Sample"');
    });

    it('should be quoted if token of path start with a number', () => {
      expect(constructFullPath(['Tphc-Sample', '2ds1'])).to.eql('"Tphc-Sample"."2ds1"');
    });

    it('should encode path if we have shouldEncode=true', () => {
      expect(constructFullPath(['Tphc-Sample'], false, true)).to.eql('%22Tphc-Sample%22');
    });

    it('should be quoted if token of path start with -', () => {
      expect(constructFullPath(['Prod', '-foo'])).to.eql('Prod."-foo"');
    });

    it('should be quoted if token is single digit', () => {
      expect(constructFullPath(['Prod', '1'])).to.eql('Prod."1"');
    });

    it('should be quoted if token of path is "-"', () => {
      expect(constructFullPath(['Prod', '-'])).to.eql('Prod."-"');
    });

    it('should not be quoted if token of path is single alpha character', () => {
      expect(constructFullPath(['Prod', 'a'])).to.eql('Prod.a');
    });

    it('should escape " as ""', () => {
      expect(constructFullPath(['Prod', 'a"'])).to.eql('Prod."a"""');
    });
  });

  describe('getInitialResourceLocation', () => {
    it('should return location as "@dremio" if fullPath is undefined', () => {
      expect(
        getInitialResourceLocation(undefined, 'VIRTUAL_DATASET', 'dremio')
      ).to.eql('"@dremio"');
    });

    it('should return location as "@dremio" if fullPath[0] is tmp', () => {
      expect(
        getInitialResourceLocation(['tmp'], 'DATASET', 'dremio')
      ).to.eql('"@dremio"');
    });

    it('should return location as "@dremio" if datasetType is not VIRTUAL_DATASET/PHYSICAL_DATASET_HOME_FILE', () => {
      expect(
        getInitialResourceLocation(['Prod-Sample'], 'PHYSICAL_DATASET', 'dremio')
      ).to.eql('"@dremio"');
    });

    it(`should return full path as Prod-Sample.ds1 if fullPath[0] is not tmp and
        datasetType is VIRTUAL_DATASET`, () => {
      expect(
        getInitialResourceLocation(['Prod-Sample', 'ds1'], 'VIRTUAL_DATASET', 'dremio')
      ).to.eql('"Prod-Sample".ds1');
    });

    it(`should return full path as Prod-Sample.ds1 if fullPath[0] is not tmp and
        datasetType is PHYSICAL_DATASET_HOME_FILE`, () => {
      expect(
        getInitialResourceLocation(['Prod-Sample', 'ds1'], 'PHYSICAL_DATASET_HOME_FILE', 'dremio')
      ).to.eql('"Prod-Sample".ds1');
    });
  });

  describe('getUniqueName', () => {
    it('should return passed in name if it is unique', () => {
      expect(getUniqueName('unique', (name) => (![].includes(name)))).to.eql('unique');
    });

    it('should return name (3)', () => {
      expect(getUniqueName('name', (name) => (!['name', 'name (1)', 'name (2)'].includes(name)))).to.eql('name (3)');
    });
  });

  describe('getRouteParamsFromLocation', () => {
    it('should return empty strings props w/o location', () => {
      const routParams = getRouteParamsFromLocation();
      expect(routParams.resourceId).to.equal('');
      expect(routParams.tableId).to.equal('');
    });

    it('should parse location pathname', () => {
      const routParams = getRouteParamsFromLocation({pathname: 'a/b/c/d'});
      expect(routParams.resourceId).to.equal('c');
      expect(routParams.tableId).to.equal('d');
    });

    it('should decode pathname', () => {
      let routParams = getRouteParamsFromLocation({pathname: 'a/b/"c"/%22d%22'});
      expect(routParams.resourceId).to.equal('"c"');
      expect(routParams.tableId).to.equal('"d"');

      const spaceName = '@dremio space';
      const datasetName = '@a test dataset';
      const pathname = ['a', 'b', spaceName, datasetName].map(encodeURIComponent).join('/');
      routParams = getRouteParamsFromLocation({ pathname });

      expect(routParams).to.be.eql({
        resourceId: spaceName,
        tableId: datasetName
      });
    });
  });

  describe('navigateToExploreDefaultIfNecessary', () => {
    const router = [];
    const location = { pathname: '/a/b/c/d', hash: ''};
    it('should not alter router for default page type', () => {
      navigateToExploreDefaultIfNecessary('default', location, router);
      expect(router.length).to.equal(0);
    });
    it('should add router entry', () => {
      navigateToExploreDefaultIfNecessary('', location, router);
      expect(router.length).to.equal(1);
      expect(router[0].hash).to.equal('');
      expect(router[0].pathname).to.equal('/a/b/c');
    });
  });
});
