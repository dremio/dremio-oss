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
import transformRules from './transformRules';

class TransformModelMapper {
  transformExportPostMapper(data) {
    if (!data) {
      return {};
    }
    const {columnName, ...otherData} = data;
    delete otherData.fullCellSelection;
    return {
      ...otherData,
      colName: columnName
    };
  }

  mapTransformValuesPreview = data => ({
    selection: this.transformExportPostMapper(data.selection),
    replacedValues: data.values
  });

  transformDynamicPreviewMapper = data => ({
    selection: this.transformExportPostMapper(data.selection),
    rule: transformRules.getRuleMapper(data.rule.type)(data.rule)
  });

}

const transformModelMapper = new TransformModelMapper();
export default transformModelMapper;
