"""
Flask-PyESMongoEngine
-------------

Extension to add PyES to Flask using Flask-MongoEngine extension. Indexing is done
via MongoDb River plugin.
"""
from setuptools import setup

setup(
  name = 'Flask-PyESMongoEngine',
  version = '0.1.0',
  url = 'https://github.com/MasterMind2k/flask-pyesmongoengine',
  license = 'BSD',
  author = 'Gregor Kalisnik',
  author_email = 'gregor@kalisnik.si',
  description = 'Adding PyES with MongoEngine to Flask, indexing with MongoDb river',
  long_description = __doc__,
  py_modules = ['flask_pyesmongoengine'],
  zip_safe = False,
  include_package_data = True,
  platforms = 'any',
  install_requires = [
    'Flask',
    'flask-mongoengine',
    'pyes'
  ],
  classifiers = [
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    'Topic :: Software Development :: Libraries :: Python Modules'
  ]
)
