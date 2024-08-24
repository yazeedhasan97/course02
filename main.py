import sys
from datetime import datetime
from argparse import Namespace, ArgumentParser

from PyQt6.QtWidgets import QApplication

from controllers.app import AppController
from models.consts import Status
from models.db import get_db_hook
from models.models import BASE
from utilities.loggings import MultipurposeLogger
from utilities.utils import load_json_file
from views.login import LoginForm


def cli() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--log", type=str, default='logs', help="Path to the log directory.")
    parser.add_argument("--config", type=str, default='configs/config.json', help="Path to the config JSON file.")
    return parser.parse_args()


def main():
    config = load_json_file(args.config)
    connection, factory = get_db_hook(
        config=config.get('local', None),
        base=BASE,
        logger=logger

    )
    factory.create_tables()

    AppController.conn = connection
    AppController.fac = factory

    app = QApplication([])

    try:
        form = LoginForm()
        # if not os.path.exists(consts.REMEMBER_ME_FILE_PATH):
        form.show()

    except Exception as e:
        print(e)
        raise e
    finally:
        pass

    code = app.exec()
    factory.close(), connection.close()
    sys.exit(code)


if __name__ == '__main__':
    args = cli()

    logger = MultipurposeLogger(
        name="DBConnectionogger",
        path=args.log
    )

    main()

    pass
