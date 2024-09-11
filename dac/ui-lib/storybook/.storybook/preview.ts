import "../../themes/utilities/reset.scss";
import "../assets/fonts/inter-ui/inter.css";
import "../assets/fonts/FiraCode/FiraCode.css";
import "../../themes/dremio/index.scss";
import { configureDremioIcon } from "../../components/icon/configureDremioIcon";
import "../../themes/dremio/components/table.scss";
import IconSpritePath from "../../dist-icons/dremio.svg";

configureDremioIcon(IconSpritePath);

export const parameters = {
  darkMode: {
    classTarget: "html",
    lightClass: "dremio-light",
    darkClass: "dremio-dark",
    stylePreview: true,
  },
};
