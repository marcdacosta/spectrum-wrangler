from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from spectrum_wrangler.db import connect, initialize
from spectrum_wrangler.mcp_server import McpServer


class McpServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "agent.sqlite3"
        with connect(self.database) as connection:
            initialize(connection)
        self.server = McpServer(self.database)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_initialize_and_tool_discovery(self) -> None:
        initialized = self.server.handle({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        })
        self.assertEqual(initialized["result"]["protocolVersion"], "2025-11-25")
        modern_initialize = self.server.handle({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "initialize",
            "params": {"protocolVersion": "2026-07-28"},
        })
        self.assertEqual(modern_initialize["result"]["protocolVersion"], "2025-11-25")
        tools = self.server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        self.assertEqual(
            {tool["name"] for tool in tools["result"]["tools"]},
            {
                "spectrum_status",
                "describe_schema",
                "query_spectrum_sql",
                "lookup_callsign",
                "search_licenses",
                "search_frequency",
                "search_nearby_sites",
                "get_license_record",
                "search_text",
                "survey_band",
                "list_radio_services",
                "list_expirations",
                "summarize_geography",
            },
        )
        self.assertTrue(all(
            tool["annotations"]["readOnlyHint"]
            for tool in tools["result"]["tools"]
        ))

        resources = self.server.handle({"jsonrpc": "2.0", "id": 3, "method": "resources/list"})
        self.assertEqual(
            {resource["uri"] for resource in resources["result"]["resources"]},
            {"spectrum://guide", "spectrum://schema", "spectrum://sources"},
        )
        prompts = self.server.handle({"jsonrpc": "2.0", "id": 4, "method": "prompts/list"})
        self.assertEqual(
            {prompt["name"] for prompt in prompts["result"]["prompts"]},
            {"investigate_spectrum"},
        )

        missing = self.server.handle({"jsonrpc": "2.0", "id": 11, "method": "not/a_method"})
        self.assertEqual(missing["error"]["code"], -32601)
        invalid = self.server.handle({
            "jsonrpc": "2.0", "id": 12, "method": "tools/list", "params": []
        })
        self.assertEqual(invalid["error"]["code"], -32602)

    def test_schema_and_sql_tools(self) -> None:
        schema = self.server.handle({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "describe_schema", "arguments": {"table": "raw_fr"}},
        })
        payload = json.loads(schema["result"]["content"][0]["text"])
        self.assertIn("frequency_assigned", {item["name"] for item in payload["columns"]})

        query = self.server.handle({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "query_spectrum_sql",
                "arguments": {"sql": "SELECT count(*) AS count FROM uls_raw_catalog"},
            },
        })
        payload = json.loads(query["result"]["content"][0]["text"])
        self.assertEqual(payload["rows"][0]["count"], 89)

        denied = self.server.handle({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "query_spectrum_sql",
                "arguments": {"sql": "DROP TABLE metadata"},
            },
        })
        self.assertTrue(denied["result"]["isError"])

    def test_sensitive_raw_fields_require_operator_opt_in(self) -> None:
        with connect(self.database) as connection:
            connection.execute(
                "INSERT INTO sources(source_key,authority,url,retrieved_at,sha256,byte_size) "
                "VALUES(?,?,?,?,?,?)",
                ("sensitive-test", "test", "https://example.test", "2026-01-01", "0" * 64, 0),
            )
            connection.execute(
                "INSERT INTO raw_en(source_archive,source_id,source_row,record_type,email) "
                "VALUES(?,?,?,?,?)",
                ("test.zip", 1, 1, "EN", "private@example.test"),
            )
            connection.commit()

        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "query_spectrum_sql",
                "arguments": {"sql": "SELECT email FROM raw_en"},
            },
        }
        denied = self.server.handle(request)
        self.assertTrue(denied["result"]["isError"])
        trusted = McpServer(self.database, allow_sensitive=True).handle(request)
        self.assertFalse(trusted["result"]["isError"])
        self.assertEqual(
            trusted["result"]["structuredContent"]["rows"][0]["email"],
            "private@example.test",
        )

if __name__ == "__main__":
    unittest.main()
