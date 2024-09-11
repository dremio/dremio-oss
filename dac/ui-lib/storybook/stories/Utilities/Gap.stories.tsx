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

export default {
  title: "Utilities/Gap",
};

const sizes = [
  "05",
  "1",
  "105",
  "2",
  "205",
  "3",
  "4",
  "405",
  "5",
  "505",
  "6",
  "7",
  "8",
  "9",
  "905",
  "10",
];

export const Default = () => {
  return (
    <div className="dremio-layout-stack max-w-max" style={{ "--space": "1em" }}>
      {sizes.map((size) => (
        <div key={size}>
          <div className={`bg-neutral-25 flex gap-${size} p-1 rounded`}>
            {Array(4).fill(
              <div className="bg-brand-200 w-2 h-2 rounded-sm"></div>,
            )}
          </div>
          <code>.gap-{size}</code>
        </div>
      ))}
    </div>
  );
};

Default.storyName = "Gap";
