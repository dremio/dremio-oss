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

import { Meta, StoryFn } from "@storybook/react";

import {
  SegmentedControl,
  SegmentedControlOption,
  useSegmentedControl,
} from "../../../components";

export default {
  title: "Input Controls/SegmentedControl",
  component: SegmentedControl,
} as Meta<typeof SegmentedControl>;

export const Default: StoryFn<typeof SegmentedControl> = () => {
  const segmentedControl1 = useSegmentedControl("1");
  const segmentedControl2 = useSegmentedControl("1");
  return (
    <div>
      <SegmentedControl {...segmentedControl1}>
        <SegmentedControlOption tooltip="Option 1" value="1">
          Option 1
        </SegmentedControlOption>
        <SegmentedControlOption tooltip="Option 2" value="2">
          Option 2
        </SegmentedControlOption>
        <SegmentedControlOption tooltip="Option 3" value="3">
          Option 3
        </SegmentedControlOption>
      </SegmentedControl>
      <br />
      <SegmentedControl {...segmentedControl2}>
        <SegmentedControlOption tooltip="Calendar" value="1">
          <dremio-icon name="column-types/datetime"></dremio-icon>
        </SegmentedControlOption>
        <SegmentedControlOption tooltip="Arctic Jobs" value="2">
          <dremio-icon name="brand/arctic-jobs"></dremio-icon>
        </SegmentedControlOption>
        <SegmentedControlOption tooltip="Table" value="3">
          <dremio-icon name="column-types/table"></dremio-icon>
        </SegmentedControlOption>
      </SegmentedControl>
    </div>
  );
};

Default.storyName = "SegmentedControl";
