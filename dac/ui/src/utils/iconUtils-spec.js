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
import { getIconDataTypeFromEntity, getIconDataTypeFromDatasetType } from './iconUtils';

describe('iconUtils', () => {

  describe('getIconDataTypeFromEntity', () => {
    it('should return Folder if fileType is folder, and not queryable', () => {
      expect(getIconDataTypeFromEntity(Immutable.Map({fileType: 'folder', queryable: false }))).to.eql('Folder');
    });
    it('should return FolderData if fileType is folder, and is queryable', () => {
      expect(getIconDataTypeFromEntity(Immutable.Map({fileType: 'folder', queryable: true }))).to.eql('FolderData');
    });
    it('should return FileEmpty if fileType is file, and not queryable', () => {
      expect(getIconDataTypeFromEntity(Immutable.Map({fileType: 'file', queryable: false }))).to.eql('FileEmpty');
    });
    it('should return File if fileType is file, and is queryable', () => {
      expect(getIconDataTypeFromEntity(Immutable.Map({fileType: 'file', queryable: true }))).to.eql('File');
    });
    it('should return PhysicalDataset if fileType is physicalDatasets', () => {
      expect(
        getIconDataTypeFromEntity(Immutable.Map({fileType: 'physicalDatasets', queryable: true }))
      ).to.eql('PhysicalDataset');
    });
  });

  describe('getIconDataTypeFromDatasetType', () => {
    it('returns correct icon type', () => {
      expect(getIconDataTypeFromDatasetType('PHYSICAL_DATASET')).to.eql('PhysicalDataset');
    });
  });
});
