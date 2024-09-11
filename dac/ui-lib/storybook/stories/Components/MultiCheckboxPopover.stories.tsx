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

import { useState } from "react";
import { Meta } from "@storybook/react";

import { MultiCheckboxPopover } from "../../../components";

export default {
  title: "Components/MultiCheckboxPopover",
  component: MultiCheckboxPopover,
} as Meta<typeof MultiCheckboxPopover>;

const items = [
  { label: "Canceled", id: "canceled" },
  { label: "Completed", id: "completed" },
  { label: "Engine Start", id: "engineStart" },
  { label: "Failed", id: "failed" },
  { label: "Queued", id: "queued" },
  { label: "Running", id: "running" },
  { label: "Setup", id: "setup" },
];

export const Default = () => {
  const [selectedItems, setSelectedItems] = useState([items[0]]);
  return (
    <MultiCheckboxPopover
      listItems={items}
      selectedtListItems={selectedItems}
      listLabel="Status"
      onItemSelect={(id) =>
        setSelectedItems((cur) => [
          ...cur,
          items.find((item) => item.id === id) ?? items[0],
        ])
      }
      onItemUnselect={(id) =>
        setSelectedItems((cur) => cur.filter((item) => item.id !== id))
      }
    />
  );
};

Default.storyName = "MultiCheckboxPopover";
