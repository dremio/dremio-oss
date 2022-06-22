import { define, SVGUseAdapter } from "smart-icon";

/**
 * Globally defines the `<dremio-icon>` custom element and configures runtime icon path resolution
 * @param iconsRoot The base HTTP path for the icons/ folder at runtime
 */
export const configureDremioIcon = (iconsRoot: string): void => {
  define("dremio-icon", {
    adapter: SVGUseAdapter,
    resolvePath: (name) => `${iconsRoot}/${name}.svg#${name}`,
  });
};
