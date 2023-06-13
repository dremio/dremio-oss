import clsx from "clsx";
import * as React from "react";

type RadioProps = {
  className?: string;
  label?: string;
};

export const Radio = (props: RadioProps) => (
  <label>
    <input
      {...props}
      className={clsx(props.className, "form-control")}
      type="radio"
    />
    {props.label}
  </label>
);
