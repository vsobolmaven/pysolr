# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import re
import requests
import time
import types
import urllib

try:
    # Prefer lxml, if installed.
    from lxml import etree as ET
except ImportError:
    try:
        from xml.etree import cElementTree as ET
    except ImportError:
        raise ImportError("No suitable ElementTree implementation was found.")

try:
    # Prefer simplejson, if installed.
    import simplejson as json
except ImportError:
    import json

try:
    # Python 3.X
    from urllib.parse import urlsplit, urlunsplit, urlencode
except ImportError:
    # Python 2.X
    from urlparse import urlsplit, urlunsplit
    from urllib import urlencode

try:
    # Python 3.X
    import html.entities as htmlentities
except ImportError:
    # Python 2.X
    import htmlentitydefs as htmlentities

try:
    # Python 2.X
    unicode_char = unichr
except NameError:
    # Python 3.X
    unicode_char = chr
    # I do NOT feel good about doing this. :/
    unicode = str
    basestring = str


__author__ = 'Daniel Lindsley, Joseph Kocherhans, Jacob Kaplan-Moss'
__all__ = ['Solr']
__version__ = (3, 0, 0)


def get_version():
    return "%s.%s.%s" % __version__[:3]


DATETIME_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$')


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


# Add the ``NullHandler`` to avoid logging by default while still allowing
# others to attach their own handlers.
LOG = logging.getLogger('pysolr')
h = NullHandler()
LOG.addHandler(h)

# For debugging...
if False:
    LOG.setLevel(logging.DEBUG)
    stream = logging.StreamHandler()
    LOG.addHandler(stream)


def unescape_html(text):
    """
    Removes HTML or XML character references and entities from a text string.

    @param text The HTML (or XML) source text.
    @return The plain text, as a Unicode string, if necessary.

    Source: http://effbot.org/zone/re-sub.htm#unescape-html
    """
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unicode_char(int(text[3:-1], 16))
                else:
                    return unicode_char(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unicode_char(htmlentities.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+;", fixup, text)


def safe_urlencode(params, doseq=0):
    """
    UTF-8-safe version of safe_urlencode

    The stdlib safe_urlencode prior to Python 3.x chokes on UTF-8 values
    which can't fail down to ascii.
    """
    if hasattr(params, "items"):
        params = params.items()

    new_params = list()

    for k, v in params:
        k = k.encode("utf-8")

        if isinstance(v, basestring):
            new_params.append((k, v.encode("utf-8")))
        elif isinstance(v, (list, tuple)):
            new_params.append((k, [i.encode("utf-8") for i in v]))
        else:
            new_params.append((k, unicode(v)))

    return urlencode(new_params, doseq)


class SolrError(Exception):
    pass


class Results(object):
    def __init__(self, docs, hits, highlighting=None, facets=None,
                 spellcheck=None, stats=None, qtime=None, debug=None,
                 grouped=None):
        self.docs = docs
        self.hits = hits
        self.highlighting = highlighting or {}
        self.facets = facets or {}
        self.spellcheck = spellcheck or {}
        self.stats = stats or {}
        self.qtime = qtime
        self.debug = debug or {}
        self.grouped = grouped or {}

    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        return iter(self.docs)


class Solr(object):
    def __init__(self, url, decoder=None, timeout=60):
        self.decoder = decoder or json.JSONDecoder()
        self.url = url
        self.timeout = timeout
        self.log = self._get_log()

    def _get_log(self):
        return LOG

    def _create_full_url(self, path=''):
        if len(path):
            return '/'.join([self.url, path.lstrip('/')])

        # No path? No problem.
        return self.url

    def _send_request(self, method, path='', body=None, headers=None):
        url = self._create_full_url(path)
        method = method.lower()
        log_body = body

        if log_body is None:
            log_body = ''

        self.log.debug("Starting request to '%s' (%s) with body '%s'...",
                       url, method, log_body[:10])
        start_time = time.time()

        try:
            requests_method = getattr(requests, method, 'get')
        except AttributeError as err:
            raise SolrError("Unable to send HTTP method '{0}.".format(method))

        try:
            resp = requests_method(url, data=body, headers=headers, timeout=self.timeout)
        except requests.exceptions.Timeout as err:
            error_message = "Connection to server '%s' timed out: %s"
            self.log.error(error_message, [url, err], exc_info=True)
            raise SolrError(error_message % [url, err])
        except requests.exceptions.ConnectionError as err:
            error_message = "Failed to connect to server at '%s', are you sure that URL is correct? Checking it in a browser might help: %s"
            params = (url, err)
            self.log.error(error_message, *params, exc_info=True)
            raise SolrError(error_message % params)

        end_time = time.time()
        self.log.info("Finished '%s' (%s) with body '%s' in %0.3f seconds.",
                      url, method, log_body[:10], end_time - start_time)

        if int(resp.status_code) != 200:
            error_message = self._extract_error(resp)
            self.log.error(error_message, extra={'data': {'headers': resp.headers,
                                                          'response': resp.content}})
            raise SolrError(error_message)

        return resp.content

    def _select(self, params):
        # specify json encoding of results
        params['wt'] = 'json'
        params_encoded = safe_urlencode(params, True)

        if len(params_encoded) < 1024:
            # Typical case.
            path = 'select/?%s' % params_encoded
            return self._send_request('get', path)
        else:
            # Handles very long queries by submitting as a POST.
            path = 'select/'
            headers = {
                'Content-type': 'application/x-www-form-urlencoded; charset=utf-8',
            }
            return self._send_request('post', path, body=params_encoded, headers=headers)

    def _mlt(self, params):
        # specify json encoding of results
        params['wt'] = 'json'
        path = 'mlt/?%s' % safe_urlencode(params, True)
        return self._send_request('get', path)

    def _suggest_terms(self, params):
        # specify json encoding of results
        params['wt'] = 'json'
        path = 'terms/?%s' % safe_urlencode(params, True)
        return self._send_request('get', path)

    def _update(self, message, clean_ctrl_chars=True, commit=True, waitFlush=None, waitSearcher=None):
        """
        Posts the given xml message to http://<self.url>/update and
        returns the result.

        Passing `sanitize` as False will prevent the message from being cleaned
        of control characters (default True). This is done by default because
        these characters would cause Solr to fail to parse the XML. Only pass
        False if you're positive your data is clean.
        """
        path = 'update/'

        # Per http://wiki.apache.org/solr/UpdateXmlMessages, we can append a
        # ``commit=true`` to the URL and have the commit happen without a
        # second request.
        query_vars = []

        if commit is not None:
            query_vars.append('commit=%s' % str(bool(commit)).lower())

        if waitFlush is not None:
            query_vars.append('waitFlush=%s' % str(bool(waitFlush)).lower())

        if waitSearcher is not None:
            query_vars.append('waitSearcher=%s' % str(bool(waitSearcher)).lower())

        if query_vars:
            path = '%s?%s' % (path, '&'.join(query_vars))

        # Clean the message of ctrl characters.
        if clean_ctrl_chars:
            message = sanitize(message)

        return self._send_request('post', path, message, {'Content-type': 'text/xml; charset=utf-8'})

    def _extract_error(self, resp):
        """
        Extract the actual error message from a solr response.
        """
        reason = resp.headers.get('reason', None)
        full_html = None

        if reason is None:
            reason, full_html = self._scrape_response(resp.headers, resp.content)

        msg = "[Reason: %s]" % reason

        if reason is None:
            msg += "\n%s" % unescape_html(full_html)

        return msg

    def _scrape_response(self, headers, response):
        """
        Scrape the html response.
        """
        # identify the responding server
        server_type = None
        server_string = headers.get('server', '')

        if server_string and 'jetty' in server_string.lower():
            server_type = 'jetty'

        if server_string and 'coyote' in server_string.lower():
            import lxml.html
            server_type = 'tomcat'

        reason = None
        full_html = ''
        dom_tree = None

        if server_type == 'tomcat':
            # Tomcat doesn't produce a valid XML response
            soup = lxml.html.fromstring(response)
            body_node = soup.find('body')
            p_nodes = body_node.cssselect('p')

            for p_node in p_nodes:
                children = p_node.getchildren()

                if len(children) >= 2 and 'message' in children[0].text.lower():
                    reason = children[1].text

            if reason is None:
                from lxml.html.clean import clean_html
                full_html = clean_html(response)
        else:
            # Let's assume others do produce a valid XML response
            try:
                dom_tree = ET.fromstring(response)
                reason_node = None

                # html page might be different for every server
                if server_type == 'jetty':
                    reason_node = dom_tree.find('body/pre')
                else:
                    reason_node = dom_tree.find('head/title')

                if reason_node is not None:
                    reason = reason_node.text

                if reason is None:
                    full_html = ET.tostring(dom_tree)
            except SyntaxError as err:
                full_html = "%s" % response

        full_html = full_html.replace('\n', '')
        full_html = full_html.replace('\r', '')
        full_html = full_html.replace('<br/>', '')
        full_html = full_html.replace('<br />', '')
        full_html = full_html.strip()
        return reason, full_html

    # Conversion #############################################################

    def _from_python(self, value):
        """
        Converts python values to a form suitable for insertion into the xml
        we send to solr.
        """
        if hasattr(value, 'strftime'):
            if hasattr(value, 'hour'):
                value = "%sZ" % value.isoformat()
            else:
                value = "%sT00:00:00Z" % value.isoformat()
        elif isinstance(value, bool):
            if value:
                value = 'true'
            else:
                value = 'false'
        elif isinstance(value, str):
            # FIXME: This is likely going to need some help to work under both 2.X & 3.X
            value = unicode(value, errors='replace')
        else:
            value = unicode(value)

        return value

    def _to_python(self, value):
        """
        Converts values from Solr to native Python values.
        """
        if isinstance(value, (int, float, long, complex)):
            return value

        if isinstance(value, (list, tuple)):
            value = value[0]

        if value == 'true':
            return True
        elif value == 'false':
            return False

        if isinstance(value, basestring):
            possible_datetime = DATETIME_REGEX.search(value)

            if possible_datetime:
                date_values = possible_datetime.groupdict()

                for dk, dv in date_values.items():
                    date_values[dk] = int(dv)

                return datetime.datetime(date_values['year'], date_values['month'], date_values['day'], date_values['hour'], date_values['minute'], date_values['second'])

        try:
            # This is slightly gross but it's hard to tell otherwise what the
            # string's original type might have been. Be careful who you trust.
            converted_value = eval(value)

            # Try to handle most built-in types.
            if isinstance(converted_value, (list, tuple, set, dict, int, float, long, complex)):
                return converted_value
        except:
            # If it fails (SyntaxError or its ilk) or we don't trust it,
            # continue on.
            pass

        return value

    def _is_null_value(self, value):
        """
        Check if a given value is ``null``.

        Criteria for this is based on values that shouldn't be included
        in the Solr ``add`` request at all.
        """
        # TODO: This should probably be removed when solved in core Solr level?
        return (value is None) or (isinstance(value, basestring) and len(value) == 0)

    # API Methods ############################################################

    def search(self, q, **kwargs):
        """Performs a search and returns the results."""
        params = {'q': q}
        params.update(kwargs)
        response = self._select(params)

        # TODO: make result retrieval lazy and allow custom result objects
        result = self.decoder.decode(response)
        result_kwargs = {}

        if result.get('debug'):
            result_kwargs['debug'] = result['debug']

        if result.get('highlighting'):
            result_kwargs['highlighting'] = result['highlighting']

        if result.get('facet_counts'):
            result_kwargs['facets'] = result['facet_counts']

        if result.get('spellcheck'):
            result_kwargs['spellcheck'] = result['spellcheck']

        if result.get('stats'):
            result_kwargs['stats'] = result['stats']

        if 'QTime' in result.get('responseHeader', {}):
            result_kwargs['qtime'] = result['responseHeader']['QTime']

        if result.get('grouped'):
            result_kwargs['grouped'] = result['grouped']

        response = result.get('response') or {}
        numFound = response.get('numFound', 0)
        self.log.debug("Found '%s' search results.", numFound)
        return Results(response.get('docs', ()), numFound, **result_kwargs)

    def more_like_this(self, q, mltfl, **kwargs):
        """
        Finds and returns results similar to the provided query.

        Requires Solr 1.3+.
        """
        params = {
            'q': q,
            'mlt.fl': mltfl,
        }
        params.update(kwargs)
        response = self._mlt(params)

        result = self.decoder.decode(response)

        if result['response'] is None:
            result['response'] = {
                'docs': [],
                'numFound': 0,
            }

        self.log.debug("Found '%s' MLT results.", result['response']['numFound'])
        return Results(result['response']['docs'], result['response']['numFound'])

    def suggest_terms(self, fields, prefix, **kwargs):
        """
        Accepts a list of field names and a prefix

        Returns a dictionary keyed on field name containing a list of
        ``(term, count)`` pairs

        Requires Solr 1.4+.
        """
        params = {
            'terms.fl': fields,
            'terms.prefix': prefix,
        }
        params.update(kwargs)
        response = self._suggest_terms(params)
        result = self.decoder.decode(response)
        terms = result.get("terms", {})
        res = {}

        # in Solr 1.x the value of terms is a flat list:
        #   ["field_name", ["dance",23,"dancers",10,"dancing",8,"dancer",6]]
        #
        # in Solr 3.x the value of terms is a dict:
        #   {"field_name": ["dance",23,"dancers",10,"dancing",8,"dancer",6]}
        if isinstance(terms, types.ListType):
            terms = dict(zip(terms[0::2], terms[1::2]))

        for field, values in terms.iteritems():
            tmp = list()

            while values:
                tmp.append((values.pop(0), values.pop(0)))

            res[field] = tmp

        self.log.debug("Found '%d' Term suggestions results.", sum(len(j) for i, j in res.items()))
        return res

    def add(self, docs, commit=True, boost=None, commitWithin=None, waitFlush=None, waitSearcher=None):
        """
        Adds or updates documents. For now, docs is a list of dictionaries
        where each key is the field name and each value is the value to index.
        """
        start_time = time.time()
        self.log.debug("Starting to build add request...")
        message = ET.Element('add')

        if commitWithin:
            message.set('commitWithin', commitWithin)

        for doc in docs:
            d = ET.Element('doc')

            for key, value in doc.items():
                if key == 'boost':
                    d.set('boost', str(value))
                    continue

                # handle lists, tuples, and other iterables
                if hasattr(value, '__iter__'):
                    for v in value:
                        if self._is_null_value(value):
                            continue

                        if boost and v in boost:
                            if not isinstance(boost, basestring):
                                boost[v] = str(boost[v])

                            f = ET.Element('field', name=key, boost=boost[v])
                        else:
                            f = ET.Element('field', name=key)

                        f.text = self._from_python(v)
                        d.append(f)
                # handle strings and unicode
                else:
                    if self._is_null_value(value):
                        continue

                    if boost and key in boost:
                        if not isinstance(boost, basestring):
                            boost[key] = str(boost[key])

                        f = ET.Element('field', name=key, boost=boost[key])
                    else:
                        f = ET.Element('field', name=key)

                    f.text = self._from_python(value)
                    d.append(f)

            message.append(d)

        m = ET.tostring(message, encoding='utf-8')
        end_time = time.time()
        self.log.debug("Built add request of %s docs in %0.2f seconds.", len(docs), end_time - start_time)
        return self._update(m, commit=commit, waitFlush=waitFlush, waitSearcher=waitSearcher)

    def delete(self, id=None, q=None, commit=True, waitFlush=None, waitSearcher=None):
        """Deletes documents."""
        if id is None and q is None:
            raise ValueError('You must specify "id" or "q".')
        elif id is not None and q is not None:
            raise ValueError('You many only specify "id" OR "q", not both.')
        elif id is not None:
            m = '<delete><id>%s</id></delete>' % id
        elif q is not None:
            m = '<delete><query>%s</query></delete>' % q

        return self._update(m, commit=commit, waitFlush=waitFlush, waitSearcher=waitSearcher)

    def commit(self, waitFlush=None, waitSearcher=None, expungeDeletes=None):
        if expungeDeletes is not None:
            msg = '<commit expungeDeletes="%s" />' % str(bool(expungeDeletes)).lower()
        else:
            msg = '<commit />'

        return self._update(msg, waitFlush=waitFlush, waitSearcher=waitSearcher)

    def optimize(self, waitFlush=None, waitSearcher=None, maxSegments=None):
        if maxSegments:
            msg = '<optimize maxSegments="%d" />' % maxSegments
        else:
            msg = '<optimize />'

        return self._update(msg, waitFlush=waitFlush, waitSearcher=waitSearcher)

    def extract(self, file_obj, extractOnly=True, **kwargs):
        """
        POSTs a file to the Solr ExtractingRequestHandler so rich content can
        be processed using Apache Tika. See the Solr wiki for details:

            http://wiki.apache.org/solr/ExtractingRequestHandler

        The ExtractingRequestHandler has a very simply model: it extracts
        contents and metadata from the uploaded file and inserts it directly
        into the index. This is rarely useful as it allows no way to store
        additional data or otherwise customize the record. Instead, by default
        we'll use the extract-only mode to extract the data without indexing it
        so the caller has the opportunity to process it as appropriate; call
        with ``extractOnly=False`` if you want to insert with no additional
        processing.

        Returns None if metadata cannot be extracted; otherwise returns a
        dictionary containing at least two keys:

            :contents:
                        Extracted full-text content, if applicable
            :metadata:
                        key:value pairs of text strings
        """
        if not hasattr(file_obj, "name"):
            raise ValueError("extract() requires file-like objects which have a defined name property")

        params = {
            "extractOnly": "true" if extractOnly else "false",
            "lowernames": "true",
            "wt": "json",
        }
        params.update(kwargs)

        try:
            # We'll provide the file using its true name as Tika may use that
            # as a file type hint:
            resp = self._send_request('post', "update/extract",
                                      body="".join(body_generator),
                                      headers=params,
                                      files={'file': (file_obj.name, file_obj)})
        except (IOError, SolrError) as err:
            self.log.error("Failed to extract document metadata: %s", err,
                           exc_info=True)
            raise

        try:
            data = json.loads(resp)
        except ValueError as err:
            self.log.error("Failed to load JSON response: %s", err,
                           exc_info=True)
            raise

        data['contents'] = data.pop(file_obj.name, None)
        data['metadata'] = metadata = {}

        raw_metadata = data.pop("%s_metadata" % file_obj.name, None)

        if raw_metadata:
            # The raw format is somewhat annoying: it's a flat list of
            # alternating keys and value lists
            while raw_metadata:
                metadata[raw_metadata.pop()] = raw_metadata.pop()

        return data


class SolrCoreAdmin(object):
    """
    Handles core admin operations: see http://wiki.apache.org/solr/CoreAdmin

    Operations offered by Solr are:
       1. STATUS
       2. CREATE
       3. RELOAD
       4. RENAME
       5. ALIAS
       6. SWAP
       7. UNLOAD
       8. LOAD (not currently implemented)
    """
    def __init__(self, url, *args, **kwargs):
        super(SolrCoreAdmin, self).__init__(*args, **kwargs)
        self.url = url

    def _get_url(self, url, params={}, headers={}):
        resp = requests.get(url, data=safe_urlencode(params), headers=headers)
        return resp.content

    def status(self, core=None):
        """http://wiki.apache.org/solr/CoreAdmin#head-9be76f5a459882c5c093a7a1456e98bea7723953"""
        params = {
            'action': 'STATUS',
        }

        if core is not None:
            params.update(core=core)

        return self._get_url(self.url, params=params)

    def create(self, name, instance_dir=None, config='solrcofig.xml', schema='schema.xml'):
        """http://wiki.apache.org/solr/CoreAdmin#head-7ca1b98a9df8b8ca0dcfbfc49940ed5ac98c4a08"""
        params = {
            'action': 'STATUS',
            'name': name,
            'config': config,
            'schema': schema,
        }

        if instance_dir is None:
            params.update(instanceDir=name)
        else:
            params.update(instanceDir=instance_dir)

        return self._get_url(self.url, params=params)

    def reload(self, core):
        """http://wiki.apache.org/solr/CoreAdmin#head-3f125034c6a64611779442539812067b8b430930"""
        params = {
            'action': 'RELOAD',
            'core': core,
        }
        return self._get_url(self.url, params=params)

    def rename(self, core, other):
        """http://wiki.apache.org/solr/CoreAdmin#head-9473bee1abed39e8583ba45ef993bebb468e3afe"""
        params = {
            'action': 'RENAME',
            'core': core,
            'other': other,
        }
        return self._get_url(self.url, params=params)

    def swap(self, core, other):
        """http://wiki.apache.org/solr/CoreAdmin#head-928b872300f1b66748c85cebb12a59bb574e501b"""
        params = {
            'action': 'SWAP',
            'core': core,
            'other': other,
        }
        return self._get_url(self.url, params=params)

    def unload(self, core):
        """http://wiki.apache.org/solr/CoreAdmin#head-f5055a885932e2c25096a8856de840b06764d143"""
        params = {
            'action': 'UNLOAD',
            'core': core,
        }
        return self._get_url(self.url, params=params)

    def load(self, core):
        raise NotImplementedError('Solr 1.4 and below do not support this operation.')


# Using two-tuples to preserve order.
REPLACEMENTS = (
    # Nuke nasty control characters.
    (b'\x00', ''), # Start of heading
    (b'\x01', ''), # Start of heading
    (b'\x02', ''), # Start of text
    (b'\x03', ''), # End of text
    (b'\x04', ''), # End of transmission
    (b'\x05', ''), # Enquiry
    (b'\x06', ''), # Acknowledge
    (b'\x07', ''), # Ring terminal bell
    (b'\x08', ''), # Backspace
    (b'\x0b', ''), # Vertical tab
    (b'\x0c', ''), # Form feed
    (b'\x0e', ''), # Shift out
    (b'\x0f', ''), # Shift in
    (b'\x10', ''), # Data link escape
    (b'\x11', ''), # Device control 1
    (b'\x12', ''), # Device control 2
    (b'\x13', ''), # Device control 3
    (b'\x14', ''), # Device control 4
    (b'\x15', ''), # Negative acknowledge
    (b'\x16', ''), # Synchronous idle
    (b'\x17', ''), # End of transmission block
    (b'\x18', ''), # Cancel
    (b'\x19', ''), # End of medium
    (b'\x1a', ''), # Substitute character
    (b'\x1b', ''), # Escape
    (b'\x1c', ''), # File separator
    (b'\x1d', ''), # Group separator
    (b'\x1e', ''), # Record separator
    (b'\x1f', ''), # Unit separator
)

def sanitize(data):
    fixed_string = data

    for bad, good in REPLACEMENTS:
        fixed_string = fixed_string.replace(bad, good)

    return fixed_string
