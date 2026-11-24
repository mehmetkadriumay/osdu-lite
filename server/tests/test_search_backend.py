"""Transport checks; these do not assert Lucene compatibility."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).parents[1]))
from search_backend import OpenSearch, BackendError


class Response:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self):
        return json.dumps(self.value).encode()


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.backend = OpenSearch("http://localhost:9200", "Tenant/unsafe:*")

    def provisioned(self):
        # Transport-only fixtures model an admitted kind without network provisioning.
        kind = "osdu:wks:master-data--Well:1.0.0"
        self.backend.indices[kind] = self.backend.kind_index(kind)
        self.backend._refresh_target()
        return kind

    def test_partition_mapping_is_safe_and_collision_resistant(self):
        self.assertRegex(self.backend.index, r"^osdu-lite-[a-f0-9]{32}$")
        self.assertNotEqual(self.backend.index, OpenSearch("http://localhost:9200", "tenant/unsafe:*").index)
        for url in ("file:///tmp/x", "http://host:9200/x", "http://user:pass@host:9200"):
            with self.assertRaises(ValueError):
                OpenSearch(url, "p")

    @patch("search_backend.urlopen")
    def test_literal_id_and_idempotent_delete(self, request):
        self.provisioned()
        request.return_value = Response({})
        self.backend.delete("p:a/b:1")
        self.assertIn("/_doc/p%3Aa%2Fb%3A1?refresh=true", request.call_args.args[0].full_url)
        request.side_effect = HTTPError("url", 404, "", {}, None)
        self.backend.delete("missing")

    @patch("search_backend.urlopen")
    def test_transport_query_and_bulk_errors(self, request):
        kind = self.provisioned()
        request.side_effect = URLError("unavailable")
        with self.assertRaises(BackendError) as error:
            self.backend.count()
        self.assertEqual(error.exception.status, 503)
        request.side_effect = HTTPError("url", 400, "", {}, None)
        with self.assertRaises(BackendError) as error:
            self.backend.search({"kind": "*", "query": "bad:("}, ["owner"], True)
        self.assertEqual(error.exception.status, 400)
        request.side_effect = None
        request.return_value = Response({"errors": True, "items": [
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}}]})
        outcomes = self.backend.bulk([{"id": "x", "kind": kind, "version": 1}])
        self.assertEqual(len(outcomes), 1)
        self.assertFalse(outcomes[0].success)
        self.assertFalse(outcomes[0].retryable)

    @patch("search_backend.urlopen")
    def test_search_filters_before_paging_and_clears_scroll(self, request):
        self.provisioned()
        request.side_effect = [
            Response({"_scroll_id": "first", "hits": {"hits": [{"_source": {"id": "1"}}]}}),
            Response({"_scroll_id": "second", "hits": {"hits": [{"_source": {"id": "2"}}]}}),
            Response({"_scroll_id": "second", "hits": {"hits": []}}),
            Response({}),
        ]
        result = self.backend.search({"kind": ["a:*", "b:*"], "query": 'data.Name:"two words"'}, ["owner"], True)
        self.assertEqual([r["id"] for r in result], ["1", "2"])
        body = json.loads(request.call_args_list[0].args[0].data)
        self.assertEqual(body["query"]["bool"]["must"], [
            {"query_string": {"query": 'data.Name:"two words"', "lenient": False}}])
        self.assertIn({"terms": {"acl.owners": ["owner"]}}, body["query"]["bool"]["filter"])
        self.assertEqual(request.call_args.args[0].method, "DELETE")
        self.assertEqual(json.loads(request.call_args.args[0].data), {"scroll_id": ["second"]})

    @patch("search_backend.urlopen")
    def test_reset_never_addresses_another_index(self, request):
        self.provisioned()
        prefix = self.backend.owned_prefix
        request.return_value = Response({})
        self.backend.reset()
        self.assertEqual(request.call_args_list[0].args[0].method, "DELETE")
        for call in request.call_args_list:
            self.assertEqual(call.args[0].full_url, "http://localhost:9200/" + prefix + "-*")
        self.assertEqual(self.backend.indices, {})
        self.assertFalse(self.backend.rebuild_required)

    @patch("search_backend.urlopen")
    def test_scroll_is_cleaned_on_failure_and_partial_results_are_not_returned(self, request):
        self.provisioned()
        request.side_effect = [
            Response({"_scroll_id": "owned", "hits": {"hits": [{"_source": {"id": "1"}}]}}),
            TimeoutError("timeout"),
            Response({}),
        ]
        with self.assertRaises(BackendError) as error:
            self.backend.search({"kind": "*"})
        self.assertEqual(error.exception.status, 503)
        self.assertEqual(request.call_args.args[0].method, "DELETE")
        self.assertEqual(json.loads(request.call_args.args[0].data), {"scroll_id": ["owned"]})

    @patch("search_backend.urlopen")
    def test_empty_partition_has_no_shards_but_count_probes_engine(self, request):
        request.return_value = Response({})
        self.assertEqual(self.backend.count(), 0)
        self.assertEqual(request.call_args.args[0].full_url, "http://localhost:9200/")
        self.assertEqual(self.backend.search({"kind": "*"}), [])
        self.assertEqual(request.call_count, 1)
        self.assertEqual(self.backend.indices, {})
