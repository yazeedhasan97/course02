from datetime import datetime
from argparse import Namespace, ArgumentParser

from models.consts import Status
from models.db import get_db_hook
from models.models import BASE, Person
from utilities.loggings import MultipurposeLogger
from utilities.utils import load_json_file


# modularity: sub-part [module] each module is responsible for specific part of the project
# OOP is not enough to achieve full modularity
# reoccurring problems - has same solution
# Design Patterns: A common solution for a highly reoccurring problem
# Immutable
# Decoration
# MVC [Model View Controller] -- Organizational Pattern  -- help maintain modulaty, easy to use and modify, easy to understand and seperate
# -- each layer handle its own error or raise them
# -- association - decomposition

def cli() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--log", type=str, default='logs', help="Path to the log directory.")
    parser.add_argument("--config", type=str, required=True, help="Path to the config JSON file.")
    return parser.parse_args()


def main():
    config = load_json_file(args.config)
    connection, factory = get_db_hook(
        config=config.get('local', None),
        base=BASE,
        logger=logger

    )
    connection.execute("DROP TABLE python.testing_table", commit=True)
    factory.create_tables()

    p1 = Person(name="Ahmad", dob=datetime(year=1997, month=10, day=19))
    factory.session.add(p1)
    factory.session.commit()

    # p1 = factory.session.query(Person).filter(Person.name == "Ahmad").first()
    # print(p1)

    # p1.status = Status.SKIPPED
    #
    # p1.name = "Younes"
    # factory.session.commit()

    # df = connection.select("select * from Customers")
    # logger.info(df)
    # logger.info('-'*100)
    #
    # res = connection.execute(
    #     """insert into Customers (first_name, last_name, age, country) values ('John', 'Doe', 25,'China')""",
    #     commit=True
    # )
    # logger.info(res)

    # df = connection.select("select * from Customers")
    # logger.info(df)
    # logger.info('-' * 100)

    factory.close(), connection.close()


if __name__ == '__main__':
    args = cli()

    logger = MultipurposeLogger(
        name="DBConnectionogger",
        path=args.log
    )

    main()

    pass
