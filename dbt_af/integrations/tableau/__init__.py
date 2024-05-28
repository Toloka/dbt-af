from importlib.util import find_spec

from dbt_af.integrations.tableau.extracts import tableau_extracts_refresh


def is_tableau_installed():
    return find_spec('tableauserverclient') is not None


__all__ = [
    'is_tableau_installed',
    'tableau_extracts_refresh',
]
