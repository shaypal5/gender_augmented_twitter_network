"""Custom exceptions for the twikwak17 package."""


class TwikwakConfigurationError(Exception):
    """An exception caused by missing or bad configuration of twikwak17."""

    MISSING_VAL_TEMP = (
        "Missing configuration value for {}. Either provide it through the CLI"
        " call or configure it at {} .")

    def __init__(self, cfg_key, cfg_fpath):
        msg = TwikwakConfigurationError.MISSING_VAL_TEMP.format(
            cfg_key, cfg_fpath)
        super().__init__(msg)
