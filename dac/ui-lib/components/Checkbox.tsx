import clsx from "clsx";
import * as React from "react";

type CheckboxProps = {
  className?: string;
  label?: string;
};

export const Checkbox = (props: CheckboxProps) => (
  <label>
    <input
      {...props}
      className={clsx(props.className, "form-control")}
      type="checkbox"
    />
    {props.label}
  </label>
);
