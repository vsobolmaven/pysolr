# -*- coding: utf-8 -*-
from __future__ import unicode_literals

try:
    import unittest2 as unittest
except ImportError:
    import unittest


from pysolr import Solr, Results, SolrError, unescape_html, safe_urlencode, sanitize


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
    def test_init(self):
        pass
