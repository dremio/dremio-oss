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
import SourceBranchPicker from '@app/pages/HomePage/components/SourceBranchPicker/SourceBranchPicker';
import { useIsDataPlaneEnabled } from 'dyn-load/utils/dataPlaneUtils';

type TreeNodeBranchPickerProps = {
    sources?: any
    node: any;
    containerEl: any;
}

//Wrapper to show/hide picker based on DCS feature flag (always hidden in OSS and Enterprise)
function TreeNodeBranchPicker({ sources, node, containerEl }: TreeNodeBranchPickerProps) {
  const show = useIsDataPlaneEnabled();
  if (!show) return null;

  const source = (sources || []).find(
    (cur: any) => cur.get('type') === 'DATAPLANE' && cur.get('name') === node.get('name')
  );
  if (!source) return null;

  return <SourceBranchPicker source={source.toJS()} anchorEl={containerEl} />;
}
export default TreeNodeBranchPicker;
