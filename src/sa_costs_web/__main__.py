from __future__ import annotations

import logging

from .app import create_app
from .config import WebConfig


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    config = WebConfig.from_args()
    configure_logging(config.log_level)
    app = create_app(config)
    app.logger.info("energy costs web listening on http://%s:%s", config.bind_host, config.bind_port)
    app.run(host=config.bind_host, port=config.bind_port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
