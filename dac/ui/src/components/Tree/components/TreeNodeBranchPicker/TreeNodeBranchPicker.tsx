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
import SourceBranchPicker from "@app/pages/HomePage/components/SourceBranchPicker/SourceBranchPicker";
import { MutableRefObject } from "react";

type TreeNodeBranchPickerProps = {
  source?: any;
  anchorRef: MutableRefObject<HTMLElement>;
  onApply?: () => void;
};

//Wrapper to show/hide picker based on DCS feature flag (always hidden in OSS and Enterprise)
function TreeNodeBranchPicker({
  source,
  anchorRef,
  onApply,
}: TreeNodeBranchPickerProps) {
  return (
    <SourceBranchPicker
      source={source}
      getAnchorEl={() => anchorRef.current}
      redirect={false}
      onApply={onApply}
    />
  );
}
export default TreeNodeBranchPicker;
