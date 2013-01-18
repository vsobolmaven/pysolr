# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime

try:
    import unittest2 as unittest
except ImportError:
    import unittest


from pysolr import Solr, Results, SolrError, unescape_html, safe_urlencode, sanitize, json


class UtilsTestCase(unittest.TestCase):
    def test_unescape_html(self):
        self.assertEqual(unescape_html('Hello &#149; world'), 'Hello \x95 world')
        self.assertEqual(unescape_html('Hello &#x64; world'), 'Hello d world')
        self.assertEqual(unescape_html('Hello &amp; ☃'), 'Hello & ☃')
        self.assertEqual(unescape_html('Hello &doesnotexist; world'), 'Hello &doesnotexist; world')

    def test_safe_urlencode(self):
        self.assertEqual(safe_urlencode({'test': 'Hello ☃! Helllo world!'}), 'test=Hello+%E2%98%83%21+Helllo+world%21')
        self.assertEqual(safe_urlencode({'test': ['Hello ☃!', 'Helllo world!']}), 'test=%5B%27Hello+%5Cxe2%5Cx98%5Cx83%21%27%2C+%27Helllo+world%21%27%5D')
        self.assertEqual(safe_urlencode({'test': ('Hello ☃!', 'Helllo world!')}), 'test=%5B%27Hello+%5Cxe2%5Cx98%5Cx83%21%27%2C+%27Helllo+world%21%27%5D')
        self.assertEqual(safe_urlencode({'test': {'Hello': '☃ or world'}}), 'test=%7Bu%27Hello%27%3A+u%27%5Cu2603+or+world%27%7D')

    def test_sanitize(self):
        self.assertEqual(sanitize(b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19h\x1ae\x1bl\x1cl\x1do\x1e\x1f'), 'hello'),


class ResultsTestCase(unittest.TestCase):
    def test_init(self):
        default_results = Results([{'id': 1}, {'id': 2}], 2)
        self.assertEqual(default_results.docs, [{'id': 1}, {'id': 2}])
        self.assertEqual(default_results.hits, 2)
        self.assertEqual(default_results.highlighting, {})
        self.assertEqual(default_results.facets, {})
        self.assertEqual(default_results.spellcheck, {})
        self.assertEqual(default_results.stats, {})
        self.assertEqual(default_results.qtime, None)
        self.assertEqual(default_results.debug, {})
        self.assertEqual(default_results.grouped, {})

        full_results = Results(
            docs=[{'id': 1}, {'id': 2}, {'id': 3}],
            hits=3,
            # Fake data just to check assignments.
            highlighting='hi',
            facets='fa',
            spellcheck='sp',
            stats='st',
            qtime='0.001',
            debug=True,
            grouped=['a']
        )
        self.assertEqual(full_results.docs, [{'id': 1}, {'id': 2}, {'id': 3}])
        self.assertEqual(full_results.hits, 3)
        self.assertEqual(full_results.highlighting, 'hi')
        self.assertEqual(full_results.facets, 'fa')
        self.assertEqual(full_results.spellcheck, 'sp')
        self.assertEqual(full_results.stats, 'st')
        self.assertEqual(full_results.qtime, '0.001')
        self.assertEqual(full_results.debug, True)
        self.assertEqual(full_results.grouped, ['a'])

    def test_len(self):
        small_results = Results([{'id': 1}, {'id': 2}], 2)
        self.assertEqual(len(small_results), 2)

        wrong_hits_results = Results([{'id': 1}, {'id': 2}, {'id': 3}], 7)
        self.assertEqual(len(wrong_hits_results), 3)

    def test_iter(self):
        long_results = Results([{'id': 1}, {'id': 2}, {'id': 3}], 3)

        to_iter = (doc for doc in long_results)
        self.assertEqual(to_iter.next(), {'id': 1})
        self.assertEqual(to_iter.next(), {'id': 2})
        self.assertEqual(to_iter.next(), {'id': 3})
        self.assertRaises(StopIteration, to_iter.next)


class SolrTestCase(unittest.TestCase):
    def setUp(self):
        super(SolrTestCase, self).setUp()
        self.solr = Solr('http://localhost:9001/solr/pysolr_tests')

    def test_init(self):
        simple_solr = Solr('http://localhost:8983/solr')
        self.assertEqual(simple_solr.url, 'http://localhost:8983/solr')
        self.assertTrue(isinstance(simple_solr.decoder, json.JSONDecoder))
        self.assertEqual(simple_solr.scheme, 'http')
        self.assertEqual(simple_solr.base_url, 'http://localhost:8983')
        self.assertEqual(simple_solr.host, 'localhost')
        self.assertEqual(simple_solr.port, 8983)
        self.assertEqual(simple_solr.path, '/solr')
        self.assertEqual(simple_solr.timeout, 60)

    def test__send_request(self):
        self.fail()

    def test__select(self):
        self.fail()

    def test__mlt(self):
        self.fail()

    def test__suggest_terms(self):
        self.fail()

    def test__update(self):
        self.fail()

    def test__extract_error(self):
        self.fail()

    def test__scrape_response(self):
        self.fail()

    def test__from_python(self):
        self.assertEqual(self.solr._from_python(datetime.date(2013, 1, 18)), '2013-01-18T00:00:00Z')
        self.assertEqual(self.solr._from_python(datetime.datetime(2013, 1, 18, 0, 30, 28)), '2013-01-18T00:30:28Z')
        self.assertEqual(self.solr._from_python(True), 'true')
        self.assertEqual(self.solr._from_python(False), 'false')
        self.assertEqual(self.solr._from_python(1), '1')
        self.assertEqual(self.solr._from_python(1.2), '1.2')
        self.assertEqual(self.solr._from_python(b'hello'), 'hello')
        self.assertEqual(self.solr._from_python('hello ☃'), 'hello ☃')

    def test__to_python(self):
        self.assertEqual(self.solr._to_python('2013-01-18T00:00:00Z'), datetime.datetime(2013, 1, 18))
        self.assertEqual(self.solr._to_python('2013-01-18T00:30:28Z'), datetime.datetime(2013, 1, 18, 0, 30, 28))
        self.assertEqual(self.solr._to_python('true'), True)
        self.assertEqual(self.solr._to_python('false'), False)
        self.assertEqual(self.solr._to_python(1), 1)
        self.assertEqual(self.solr._to_python(1.2), 1.2)
        self.assertEqual(self.solr._to_python(b'hello'), 'hello')
        self.assertEqual(self.solr._to_python('hello ☃'), 'hello ☃')
        self.assertEqual(self.solr._to_python(['foo', 'bar']), 'foo')
        self.assertEqual(self.solr._to_python(('foo', 'bar')), 'foo')

    def test__is_null_value(self):
        self.fail()

    def test_search(self):
        self.fail()

    def test_more_like_this(self):
        self.fail()

    def test_suggest_terms(self):
        self.fail()

    def test_add(self):
        self.fail()

    def test_delete(self):
        self.fail()

    def test_commit(self):
        self.fail()

    def test_optimize(self):
        self.fail()

    def test_extract(self):
        self.fail()
