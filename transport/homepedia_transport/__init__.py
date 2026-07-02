"""Pipeline gold transport Homepedia : silver parquet duckpipe -> PostGIS.

La couche silver (`transport_commune`, produite par duckpipe) est lue depuis les
parquet locaux, puis chargée telle quelle dans la table de service lue par l'API.
duckpipe est la source de vérité ; PostGIS n'est qu'un cache de service.
"""

from .pg import connect, dsn
from .silver import read_silver

__all__ = ["connect", "dsn", "read_silver"]
