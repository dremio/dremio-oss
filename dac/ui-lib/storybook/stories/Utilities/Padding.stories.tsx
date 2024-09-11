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
  title: "Utilities/Padding",
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
    <>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 p-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.p-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 px-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.px-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 py-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.py-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 pl-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.pl-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 pr-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.pr-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 pt-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.pt-{size}</code>
            </div>
          </div>
        ))}
      </div>
      <div>
        {sizes.map((size) => (
          <div
            key={size}
            className={`bg-neutral-25 m-4 pb-${size} rounded`}
            style={{ display: "inline-flex" }}
          >
            <div className={`bg-brand-200 rounded`}>
              <code>.pb-{size}</code>
            </div>
          </div>
        ))}
      </div>
    </>
  );
};

Default.storyName = "Padding";
