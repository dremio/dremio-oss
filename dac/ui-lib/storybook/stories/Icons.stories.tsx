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
import icons from "../../iconmanifest.json";
import IconSpritePath from "../../dist-icons/dremio/sprite.svg";
import { define, SVGUseAdapter } from "smart-icon";

define("dremio-icon-sprite", {
  adapter: SVGUseAdapter,
  resolvePath: (name: string) => `${IconSpritePath}#${name}`,
});

export default {
  title: "Icons",
};

export const Icons = () => (
  <ul className="flex flex-wrap">
    {icons.map((icon) => (
      <li
        key={icon.name}
        style={{
          display: "inline-flex",
          width: "200px",
          textAlign: "center",
          fontSize: "14px",
          userSelect: "all",
        }}
        className="flex-col text-center m-1 p-2 bg-neutral-25 items-center dremio-typography-monospace"
      >
        <dremio-icon
          name={icon.name}
          style={{ width: "24px", height: "24px" }}
        ></dremio-icon>
        <span style={{ color: "var(--color--neutral--600)" }} className="mt-3">
          {icon.name}
        </span>
      </li>
    ))}
  </ul>
);

export const IconsSprite = () => {
  return (
    <ul className="flex flex-wrap">
      {icons.map((icon) => (
        <li
          key={icon.name}
          style={{
            display: "inline-flex",
            width: "200px",
            textAlign: "center",
            fontSize: "14px",
            userSelect: "all",
          }}
          className="flex-col text-center m-1 p-2 bg-neutral-25 items-center dremio-typography-monospace"
        >
          <dremio-icon-sprite
            name={icon.name}
            style={{ width: "24px", height: "24px" }}
          ></dremio-icon-sprite>
          <span
            style={{ color: "var(--color--neutral--600)" }}
            className="mt-3"
          >
            {icon.name}
          </span>
        </li>
      ))}
    </ul>
  );
};
