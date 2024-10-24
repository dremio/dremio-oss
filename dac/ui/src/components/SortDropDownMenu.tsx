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

import { Divider } from "@mui/material";

import Menu from "./Menus/Menu";
import MenuItem from "./Menus/MenuItem";
import DropdownMenu from "./Menus/DropdownMenu";

import "#oss/components/SortDropDownMenu.less";

export type SortDropDownMenuProps = {
  menuList: ({ category: string; dir: string; compare: any } | null)[];
  sortValue: { category: string; dir: string; compare: any } | null;
  disabled: boolean;
  setSortValue: any;
  selectClass: undefined | string;
};

const SortDropDownMenu = (props: SortDropDownMenuProps) => {
  const { menuList, sortValue, disabled, setSortValue, selectClass } = props;

  const SortMenu = (menuProps: any): React.ReactElement => {
    const handleClick = (selectedSort: any): void => {
      setSortValue(selectedSort);
      menuProps.closeMenu();
    };

    return (
      <Menu>
        {menuList.map((sortOptions: any) => {
          return sortOptions === null ? (
            <Divider className="custom-menu-divider" />
          ) : (
            <MenuItem
              key={`${sortOptions.category}-${sortOptions.dir}`}
              className={`${sortOptions === sortValue ? "--selected" : ""}`}
              onClick={(): void => handleClick(sortOptions)}
            >
              {sortOptions.category}
              <dremio-icon
                name="interface/sort-up"
                alt="Sort Arrow"
                class={`--sortArrow --${sortOptions.dir}`}
              />
            </MenuItem>
          );
        })}
      </Menu>
    );
  };

  return (
    <DropdownMenu
      className="sortDropDownMenu__sortMenu"
      text={sortValue ? sortValue.category : ""}
      disabled={disabled}
      hideArrow
      customImage={
        <dremio-icon
          name="interface/sort-up"
          alt="Sort Arrow"
          class={`--sortArrow --${sortValue ? sortValue.dir : ""}`}
        />
      }
      menu={<SortMenu />}
      listClass="sortDropDownMenu__sortMenu__dropdown"
      selectClass={selectClass || ""}
    />
  );
};

export default SortDropDownMenu;
