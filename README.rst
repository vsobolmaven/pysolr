======
pysolr
======

``pysolr`` is a lightweight Python wrapper for Apache Solr. It provides an
interface that queries the server and returns results based on the query.


Features
========

* Basic operations such as selecting, updating & deleting.
* Index optimization.
* "More Like This" support (if setup in Solr).
* Spelling correction (if setup in Solr).
* Timeout support.


Requirements
============

* Python 2.6-3.3
* Requests 1.0+
* **Optional** - ``lxml``
* **Optional** - ``simplejson``
* **Optional** - ``BeautifulSoup`` for Tomcat error support


Installation
============

``sudo python setup.py install`` or drop the ``pysolr.py`` file anywhere on your
PYTHONPATH.


Usage
=====

Basic usage looks like::

    # If on Python 2.X
    from __future__ import print_function
    import pysolr

    solr = pysolr.Solr('http://localhost:8983/solr/')
    results = solr.search('bananas')

    print("Saw {0} result(s).".format(len(results)))

    for result in results:
        print("The title is '{0}'.".format(result['title'])


LICENSE
=======

``pysolr`` is licensed under the New BSD license.
