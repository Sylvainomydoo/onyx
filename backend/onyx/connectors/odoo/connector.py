"""
Odoo 17 Connector for Onyx (with XML-RPC Pagination)

This connector:
- Connects via XML-RPC
- Fetches helpdesk.ticket and project.task in a paginated manner
- For each record, reads message_ids (IDs) from mail.message in smaller chunks
- Supports both bulk load (load_from_state) and incremental load (poll_source)
"""

import xmlrpc.client
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional

from onyx.connectors.interfaces import (
    LoadConnector,
    PollConnector,
    GenerateDocumentsOutput,
    SecondsSinceUnixEpoch
)
from onyx.connectors.exceptions import (
    ConnectorMissingCredentialError,
    ConnectorValidationError,
    UnexpectedValidationError
)
from onyx.connectors.models import Document, Section
from onyx.configs.constants import DocumentSource
from onyx.utils.logger import setup_logger

logger = setup_logger()


class OdooConnector(LoadConnector, PollConnector):
    """
    Onyx Connector for Odoo 17, using XML-RPC with pagination (limit/offset).
    Fetches:
      - helpdesk.ticket
      - project.task
    And reads mail.message for each record's message_ids.
    """

    def __init__(
        self,
        odoo_url: Optional[str] = None,
        odoo_db: Optional[str] = None,
        batch_size: int = 20,
        include_tickets: bool = True,
        include_tasks: bool = True,
        # Contrôle pagination côté Odoo
        odoo_page_size: int = 100,
    ) -> None:
        """
        :param odoo_url: e.g. "https://mycompany.odoo.com"
        :param odoo_db: The Odoo database name, e.g. "mycompany-main-db"
        :param batch_size: Number of Document objects per batch (Onyx-level batching)
        :param include_tickets: Whether to fetch helpdesk tickets
        :param include_tasks: Whether to fetch project tasks
        :param odoo_page_size: How many records to fetch at once from Odoo (limit) to avoid large volume issues
        """
        self.odoo_url = odoo_url
        self.odoo_db = odoo_db
        self.batch_size = batch_size
        self.include_tickets = include_tickets
        self.include_tasks = include_tasks
        self.odoo_page_size = odoo_page_size

        self.odoo_login: str | None = None
        self.odoo_password: str | None = None
        self.odoo_uid: int | None = None  # returned by authenticate

    # ------------------------------------------------
    # CREDENTIALS + VALIDATION
    # ------------------------------------------------
    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        """
        Expects something like:
        {
          "odoo_login": "john@company.com",
          "odoo_password": "SECRET"
        }
        """
        if "odoo_login" not in credentials or "odoo_password" not in credentials:
            raise ConnectorMissingCredentialError(
                "Missing 'odoo_login' or 'odoo_password' in credentials."
            )

        self.odoo_login = credentials["odoo_login"]
        self.odoo_password = credentials["odoo_password"]

        logger.info("Odoo credentials loaded.")
        return None

    def validate_connector_settings(self) -> None:
        """
        Ensures odoo_url, odoo_db, login, password are present
        and attempts an authentication to Odoo.
        """
        if not self.odoo_url:
            raise ConnectorValidationError("Missing 'odoo_url'.")
        if not self.odoo_db:
            raise ConnectorValidationError("Missing 'odoo_db'.")
        if not self.odoo_login or not self.odoo_password:
            raise ConnectorMissingCredentialError("Missing Odoo login/password.")

        try:
            self._authenticate()
        except Exception as e:
            logger.exception("Odoo validation error.")
            raise UnexpectedValidationError(f"Failed to authenticate with Odoo: {e}")

        logger.info("Odoo connector settings validated successfully.")

    # ------------------------------------------------
    # ONYX METHODS
    # ------------------------------------------------
    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Bulk load: fetch all tickets/tasks in a paginated manner.
        """
        logger.info("Starting load_from_state (Odoo).")
        self._authenticate()

        all_docs: List[Document] = []
        if self.include_tickets:
            ticket_docs = self._fetch_tickets_paginated()
            all_docs.extend(ticket_docs)

        if self.include_tasks:
            task_docs = self._fetch_tasks_paginated()
            all_docs.extend(task_docs)

        # Onyx-level batching
        for i in range(0, len(all_docs), self.batch_size):
            yield all_docs[i : i + self.batch_size]

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        """
        Incremental load: fetch only records updated between start and end,
        in a paginated manner.
        """
        start_dt = datetime.utcfromtimestamp(start).replace(tzinfo=timezone.utc)
        end_dt = datetime.utcfromtimestamp(end).replace(tzinfo=timezone.utc)

        logger.info(f"Polling Odoo from {start_dt.isoformat()} to {end_dt.isoformat()}")
        self._authenticate()

        all_docs: List[Document] = []
        if self.include_tickets:
            docs_tickets = self._fetch_tickets_paginated(since=start_dt, until=end_dt)
            all_docs.extend(docs_tickets)

        if self.include_tasks:
            docs_tasks = self._fetch_tasks_paginated(since=start_dt, until=end_dt)
            all_docs.extend(docs_tasks)

        for i in range(0, len(all_docs), self.batch_size):
            yield all_docs[i : i + self.batch_size]

    # ------------------------------------------------
    # PAGINATED FETCH FOR HELP DESK TICKETS
    # ------------------------------------------------
    def _fetch_tickets_paginated(
        self, since: datetime | None = None, until: datetime | None = None
    ) -> List[Document]:
        """
        Paginated retrieval of helpdesk.ticket
        (search with limit/offset).
        """
        logger.debug("Fetching Odoo helpdesk tickets (paginated).")
        obj = self._xmlrpc_object()

        domain = self._build_date_domain(since, until)

        offset = 0
        ticket_ids_all: List[int] = []

        # Boucle pour récupérer tous les ticket_ids en plusieurs "pages"
        while True:
            partial_ids = obj.execute_kw(
                self.odoo_db,
                self.odoo_uid,
                self.odoo_password,
                "helpdesk.ticket",
                "search",
                [domain],
                {"limit": self.odoo_page_size, "offset": offset},
            )
            if not partial_ids:
                break
            ticket_ids_all.extend(partial_ids)
            offset += self.odoo_page_size

        if not ticket_ids_all:
            logger.info("No helpdesk tickets found.")
            return []

        # Ensuite, on lit les tickets par sous-lots pour éviter un 'read' trop massif
        tickets_data: List[dict[str, Any]] = []
        for i in range(0, len(ticket_ids_all), self.odoo_page_size):
            chunk_ids = ticket_ids_all[i : i + self.odoo_page_size]
            fields_to_read = ["name", "description", "write_date", "message_ids"]
            chunk_data = obj.execute_kw(
                self.odoo_db,
                self.odoo_uid,
                self.odoo_password,
                "helpdesk.ticket",
                "read",
                [chunk_ids],
                {"fields": fields_to_read},
            )
            tickets_data.extend(chunk_data)

        # Convert each ticket into a Document
        docs: List[Document] = []
        for ticket in tickets_data:
            doc_date = self._parse_odoo_datetime(ticket.get("write_date"))
            msg_ids = ticket.get("message_ids", [])
            email_bodies, comment_bodies = self._fetch_mail_messages(msg_ids)

            doc = Document(
                id=f"odoo_ticket_{ticket['id']}",
                sections=[Section(link="", text=ticket.get("description") or "")],
                source=DocumentSource.ODOO,
                semantic_identifier=ticket.get("name", f"Ticket {ticket['id']}"),
                doc_updated_at=doc_date or datetime.now(timezone.utc),
                metadata={
                    "ticket_id": str(ticket["id"]),
                    "emails": "\n".join(email_bodies),
                    "internal_notes": "\n".join(comment_bodies),
                },
            )
            docs.append(doc)

        logger.info(f"Fetched {len(docs)} tickets from Odoo (helpdesk.ticket).")
        return docs

    # ------------------------------------------------
    # PAGINATED FETCH FOR PROJECT TASKS
    # ------------------------------------------------
    def _fetch_tasks_paginated(
        self, since: datetime | None = None, until: datetime | None = None
    ) -> List[Document]:
        """
        Paginated retrieval of project.task
        """
        logger.debug("Fetching Odoo project tasks (paginated).")
        obj = self._xmlrpc_object()

        domain = self._build_date_domain(since, until)

        offset = 0
        task_ids_all: List[int] = []

        while True:
            partial_ids = obj.execute_kw(
                self.odoo_db,
                self.odoo_uid,
                self.odoo_password,
                "project.task",
                "search",
                [domain],
                {"limit": self.odoo_page_size, "offset": offset},
            )
            if not partial_ids:
                break
            task_ids_all.extend(partial_ids)
            offset += self.odoo_page_size

        if not task_ids_all:
            logger.info("No project tasks found.")
            return []

        tasks_data: List[dict[str, Any]] = []
        for i in range(0, len(task_ids_all), self.odoo_page_size):
            chunk_ids = task_ids_all[i : i + self.odoo_page_size]
            fields_to_read = ["name", "description", "write_date", "message_ids"]
            chunk_data = obj.execute_kw(
                self.odoo_db,
                self.odoo_uid,
                self.odoo_password,
                "project.task",
                "read",
                [chunk_ids],
                {"fields": fields_to_read},
            )
            tasks_data.extend(chunk_data)

        docs: List[Document] = []
        for task in tasks_data:
            doc_date = self._parse_odoo_datetime(task.get("write_date"))
            msg_ids = task.get("message_ids", [])
            email_bodies, comment_bodies = self._fetch_mail_messages(msg_ids)

            doc = Document(
                id=f"odoo_task_{task['id']}",
                sections=[Section(link="", text=task.get("description") or "")],
                source=DocumentSource.ODOO,
                semantic_identifier=task.get("name", f"Task {task['id']}"),
                doc_updated_at=doc_date or datetime.now(timezone.utc),
                metadata={
                    "task_id": str(task["id"]),
                    "emails": "\n".join(email_bodies),
                    "internal_notes": "\n".join(comment_bodies),
                },
            )
            docs.append(doc)

        logger.info(f"Fetched {len(docs)} tasks from Odoo (project.task).")
        return docs

    # ------------------------------------------------
    # FETCH MAIL MESSAGES
    # ------------------------------------------------
    def _fetch_mail_messages(self, message_ids: List[int]) -> tuple[List[str], List[str]]:
        """
        For a list of mail.message IDs, reads them by chunk,
        collecting message_type='email' or 'comment'.
        """
        if not message_ids:
            return [], []

        obj = self._xmlrpc_object()

        all_emails: List[str] = []
        all_comments: List[str] = []

        # Paginer également la lecture de mail.message si la liste est longue
        for i in range(0, len(message_ids), self.odoo_page_size):
            chunk_ids = message_ids[i : i + self.odoo_page_size]
            fields_to_read = ["message_type", "body"]
            chunk_data = obj.execute_kw(
                self.odoo_db,
                self.odoo_uid,
                self.odoo_password,
                "mail.message",
                "read",
                [chunk_ids],
                {"fields": fields_to_read},
            )

            for msg in chunk_data:
                mtype = msg.get("message_type")
                body = msg.get("body") or ""
                if mtype == "email":
                    all_emails.append(body)
                elif mtype == "comment":
                    all_comments.append(body)

        return all_emails, all_comments

    # ------------------------------------------------
    # UTILS
    # ------------------------------------------------
    def _authenticate(self) -> None:
        """
        Authenticates once and stores uid
        """
        if self.odoo_uid is not None:
            return  # Already done

        if not self.odoo_url or not self.odoo_db:
            raise ConnectorValidationError("Missing odoo_url or odoo_db.")

        common_proxy = xmlrpc.client.ServerProxy(f"{self.odoo_url}/xmlrpc/2/common")
        uid = common_proxy.authenticate(
            self.odoo_db,
            self.odoo_login,
            self.odoo_password,
            {}
        )
        if not uid:
            raise UnexpectedValidationError("Authentication to Odoo failed.")
        self.odoo_uid = uid
        logger.info(f"Authenticated to Odoo (uid={uid}).")

    def _xmlrpc_object(self):
        """
        Returns a proxy for 'object' calls (execute_kw)
        """
        if not self.odoo_url:
            raise ConnectorValidationError("No odoo_url.")
        return xmlrpc.client.ServerProxy(f"{self.odoo_url}/xmlrpc/2/object")

    def _build_date_domain(
        self, since: datetime | None, until: datetime | None
    ) -> list:
        """
        Build an Odoo domain for filtering by write_date in the range [since, until].
        """
        domain = []
        if since:
            domain.append(("write_date", ">=", since.isoformat()))
        if until:
            domain.append(("write_date", "<=", until.isoformat()))
        return domain

    def _parse_odoo_datetime(self, dt_str: str | None) -> datetime | None:
        """
        Convert Odoo datetime string '2025-03-10 11:34:15' -> Python datetime (UTC).
        """
        if not dt_str:
            return None
        try:
            from datetime import datetime
            naive_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            return naive_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning(f"Failed to parse Odoo datetime: {dt_str}")
            return None


if __name__ == "__main__":
    """
    Example usage for local testing.
    (Set PYTHONPATH=onyx/backend if needed)
    """
    import time

    connector = OdooConnector(
        odoo_url="https://mycompany.odoo.com",
        odoo_db="mycompany-db",
        batch_size=5,
        include_tickets=True,
        include_tasks=True,
        odoo_page_size=50,  # example: each search/read limited to 50
    )
    connector.load_credentials({
        "odoo_login": "user@mycompany.com",
        "odoo_password": "SUPERSECRET"
    })
    connector.validate_connector_settings()

    print("\n=== load_from_state ===")
    all_docs_iterator = connector.load_from_state()
    first_batch = next(all_docs_iterator, [])
    print(f"First batch of docs (count={len(first_batch)}).")

    print("\n=== poll_source (last 24h) ===")
    now_ts = time.time()
    day_ago_ts = now_ts - 86400
    polled_iterator = connector.poll_source(day_ago_ts, now_ts)
    first_polled_batch = next(polled_iterator, [])
    print(f"First polled batch (count={len(first_polled_batch)}).")
