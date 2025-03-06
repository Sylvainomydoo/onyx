import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

from onyx.connectors.odoo.connector import OdooConnector
from onyx.connectors.exceptions import (
    ConnectorValidationError,
    ConnectorMissingCredentialError,
    UnexpectedValidationError
)
from onyx.connectors.models import Document
from onyx.configs.constants import DocumentSource


@pytest.fixture
def mock_xmlrpc_server():
    """
    Cette fixture intercepte xmlrpc.client.ServerProxy
    pour que toute création de ServerProxy(...) renvoie un mock.
    """
    with patch("onyx.connectors.odoo.connector.xmlrpc.client.ServerProxy") as mock_proxy_cls:
        mock_server_instance = MagicMock()
        mock_proxy_cls.return_value = mock_server_instance

        # Simule l'authentification
        def mock_authenticate(db, login, pwd, _):
            if db == "invalid-db":
                return False
            return 42  # UID fictif

        mock_server_instance.authenticate.side_effect = mock_authenticate

        yield mock_server_instance


def test_load_from_state_realistic(mock_xmlrpc_server):
    """
    Test plus réaliste : le connecteur lit un helpdesk.ticket ou project.task, récupère 'message_ids',
    puis lit mail.message pour chaque ID.
    """
    connector = OdooConnector(
        odoo_url="https://mycompany.odoo.com",
        odoo_db="my_db",
        batch_size=2,
        include_tickets=True,
        include_tasks=True,
    )
    connector.load_credentials({
        "odoo_login": "user@company.com",
        "odoo_password": "secret_pwd"
    })

    # On simule plusieurs appels :
    #  - search sur helpdesk.ticket
    #  - read sur helpdesk.ticket
    #  - search sur project.task
    #  - read sur project.task
    #  - read sur mail.message (pour les IDs trouvés dans message_ids)
    mock_xmlrpc_server.execute_kw.side_effect = _mock_realistic_side_effect

    doc_batches = connector.load_from_state()
    first_batch = next(doc_batches, [])
    second_batch = next(doc_batches, [])
    third_batch = next(doc_batches, None)

    # On s'attend à 2 batchs (2 docs chaque fois), puis plus rien
    assert len(first_batch) == 2
    assert len(second_batch) == 2
    assert third_batch is None

    # Vérif sur le contenu
    # On peut imaginer que le connecteur compile emails & notes internes dans metadata
    doc0 = first_batch[0]
    assert doc0.source == DocumentSource.ODOO
    # Par exemple, doc0.metadata["emails"] == "Liste des mails"
    # doc0.metadata["internal_notes"] == "Liste des notes"

def test_poll_source_realistic(mock_xmlrpc_server):
    """
    Vérifie la logique de poll_source (incrémental) avec deux appels successifs (tickets, tasks) et mail.message.
    """
    connector = OdooConnector(
        odoo_url="https://mycompany.odoo.com",
        odoo_db="my_db",
        batch_size=3,
        include_tickets=True,
        include_tasks=True,
    )
    connector.load_credentials({
        "odoo_login": "user@company.com",
        "odoo_password": "secret_pwd"
    })

    now_ts = datetime.now(timezone.utc).timestamp()
    start_ts = (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()

    mock_xmlrpc_server.execute_kw.side_effect = _mock_realistic_side_effect

    docs_iter = connector.poll_source(start_ts, now_ts)
    first_batch = next(docs_iter, [])
    second_batch = next(docs_iter, [])
    third_batch = next(docs_iter, None)

    assert len(first_batch) == 3  # 3 docs
    assert len(second_batch) == 1
    assert third_batch is None


def _mock_realistic_side_effect(db, uid, pwd, model, method, args, kwargs=None):
    """
    Side effect 'réaliste' :
      - "helpdesk.ticket" search -> renvoie [101, 102]
      - "helpdesk.ticket" read   -> renvoie un 'message_ids' = [201,202,...]
      - "project.task" search  -> renvoie [2010, 2011]
      - "project.task" read    -> renvoie 'message_ids' = [9999] ...
      - "mail.message" read    -> renvoie les messages e-mail ou comment

    Les IDs renvoyés dans 'message_ids' seront re-lus ensuite via mail.message
    """
    if model == "helpdesk.ticket" and method == "search":
        # Domain => on s'en fiche ici, on renvoie 2 tickets
        return [101, 102]

    elif model == "helpdesk.ticket" and method == "read":
        # On renvoie 2 tickets, chacun avec un champ message_ids
        return [
            {
                "id": 101,
                "name": "Ticket #101",
                "write_date": "2025-03-10 10:00:00",
                "description": "Desc T101",
                "message_ids": [5001, 5002],  # on simule 2 messages
            },
            {
                "id": 102,
                "name": "Ticket #102",
                "write_date": "2025-03-10 11:00:00",
                "description": "Desc T102",
                "message_ids": [5003],
            },
        ]

    elif model == "project.task" and method == "search":
        return [2010, 2011]

    elif model == "project.task" and method == "read":
        return [
            {
                "id": 2010,
                "name": "Task #2010",
                "write_date": "2025-03-10 09:00:00",
                "description": "Desc Task2010",
                "message_ids": [5010, 5011],
            },
            {
                "id": 2011,
                "name": "Task #2011",
                "write_date": "2025-03-10 09:30:00",
                "description": "Desc Task2011",
                "message_ids": [],
            },
        ]

    elif model == "mail.message" and method == "read":
        # On regarde la liste d'IDs passée en argument
        message_ids = args[0]  # ex. [5001, 5002]
        all_messages_data = {
            5001: {"id": 5001, "message_type": "email",   "body": "User email 5001" },
            5002: {"id": 5002, "message_type": "comment", "body": "Internal note 5002"},
            5003: {"id": 5003, "message_type": "email",   "body": "Another user email 5003"},
            5010: {"id": 5010, "message_type": "comment", "body": "Task comment 5010"},
            5011: {"id": 5011, "message_type": "email",   "body": "Task email 5011"},
            # ... on peut ajouter d'autres si besoin
        }
        # On renvoie un array de dicts
        return [all_messages_data[msg_id] for msg_id in message_ids if msg_id in all_messages_data]

    else:
        # Pour toute autre requête
        return []
