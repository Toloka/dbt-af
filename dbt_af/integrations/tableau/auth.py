import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tableauserverclient.models.tableau_auth import Credentials

    from dbt_af.conf import Config


def get_tableau_auth_object(config: 'Config') -> 'Credentials':
    import tableauserverclient as tsc

    if not config.tableau:
        raise ValueError('No tableau configuration found in dbt_af config. Please specify tableau credentials.')

    if config.tableau.username and config.tableau.password:
        logging.info('Using tableau username/password authentication')
        return tsc.TableauAuth(
            username=config.tableau.username,
            password=config.tableau.password,
            site_id=config.tableau.site_id,
            user_id_to_impersonate=config.tableau.user_id_to_impersonate,
        )

    if config.tableau.token_name and config.tableau.pat:
        logging.info('Using tableau personal access token authentication')
        return tsc.PersonalAccessTokenAuth(
            token_name=config.tableau.token_name,
            personal_access_token=config.tableau.pat,
            site_id=config.tableau.site_id,
        )

    raise ValueError(
        'No valid tableau authentication method found in dbt_af config. Please specify tableau credentials.'
    )
