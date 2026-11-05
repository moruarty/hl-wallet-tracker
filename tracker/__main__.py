import logging

from .bot import TrackerBot
from .config import load_config


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    TrackerBot(cfg).run()


if __name__ == "__main__":
    main()
