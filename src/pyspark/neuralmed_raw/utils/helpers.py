import logging
from datetime import datetime
from typing import Any, List

from pyspark.sql.types import Row


def parse_json(row: Row, target_columns: List[Any]):
    """
    todo
    """

    parsed_row = []
    for col_name in list(row.asDict()):
        target_datatype = [d[1] for i, d in enumerate(target_columns) if col_name in d][0]
        if target_datatype == "string":
            parsed_row.append(str(row[col_name]))
        elif target_datatype == "datetime":
            parsed_row.append(datetime.fromtimestamp(row[col_name]))  # type: ignore
        elif target_datatype == "date":
            parsed_row.append(datetime.strptime(row[col_name], "%Y-%m-%d").date())  # type: ignore # noqa
        elif target_datatype == "boolean":
            parsed_row.append(row[col_name] == "True")
        else:
            logging.info("invalid datatype")
    return tuple(parsed_row)
