from itertools import chain
from typing import Dict

from sqlalchemy import Table, MetaData, text

from .engine import engine
from .table import load_simple_table_from_configuration
from ..configuration.model import ComponentsConfiguration


def sync_db_from_configuration(
        configuration: ComponentsConfiguration,
) -> Dict[str, Table]:
    """
    Sync the database from the components' configuration.
    :param configuration: The components configuration
    :return: The tables
    """

    metadata_obj = MetaData()

    tables = {}
    indexes = []
    for name, component in chain(
            configuration.harvesters.items(),
            configuration.collectors.items(),
    ):
        tables[name] = load_simple_table_from_configuration(
            component.name, metadata_obj
        )
        # create index on
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS {component.name}_date_index ON {name} (date)"
        )

    metadata_obj.create_all(engine, checkfirst=True)

    with engine.connect() as connection:
        for index in indexes:
            connection.execute(text(index))
        connection.commit()

    return tables
