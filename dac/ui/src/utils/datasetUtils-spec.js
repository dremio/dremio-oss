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
import Immutable from 'immutable';

import {abilities} from './datasetUtils';


describe('datasetUtils', () => {
  describe('#abilities', () => {
    let entity;

    beforeEach(() => {
      entity = Immutable.Map({
        isHomeFile: false
      });
    });

    it('with a file', () => {
      expect(abilities(entity.set('entityType', 'file'))).to.eql({
        canEditFormat: true,
        canRemoveFormat: true,
        canEdit: false,
        canMove: false,
        canDelete: false,
        canSetAccelerationUpdates: true
      });
    });

    it('with a file in home', () => {
      expect(abilities(entity.set('isHomeFile', true).set('entityType', 'file'))).to.eql({
        canEditFormat: true,
        canRemoveFormat: false,
        canEdit: false,
        canMove: false,
        canDelete: true,
        canSetAccelerationUpdates: false
      });
    });

    it('with a folder', () => {
      expect(abilities(entity.set('entityType', 'folder'))).to.eql({
        canEditFormat: true,
        canRemoveFormat: true,
        canEdit: false,
        canMove: false,
        canDelete: false,
        canSetAccelerationUpdates: true
      });
    });

    it('with a physicalDataset', () => {
      expect(abilities(entity.set('entityType', 'physicalDataset'))).to.eql({
        canEditFormat: false,
        canRemoveFormat: false,
        canEdit: false,
        canMove: false,
        canDelete: false,
        canSetAccelerationUpdates: true
      });
    });

    it('with anything else', () => {
      expect(abilities(entity.set('entityType', '*'))).to.eql({
        canEditFormat: false,
        canRemoveFormat: false,
        canEdit: true,
        canMove: true,
        canDelete: true,
        canSetAccelerationUpdates: false
      });

    });
  });
});
