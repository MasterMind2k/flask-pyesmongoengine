from __future__ import absolute_import

import pyes
import pyes.aggs
import pyes.rivers
import pyes.exceptions
import copy

def get_index_name(model):
  if isinstance(model, str):
    return model

  if 'elastic_search' not in model._meta:
    return None

  return model._meta['elastic_search'].get('name', model._get_collection_name().replace('.', '_'))

def get_type(model):
  if 'elastic_search' not in model._meta or 'river' not in model._meta['elastic_search']:
    return None

  return model._get_collection_name().replace('.', '_')

def prepare_index(model):
  index_name = get_index_name(model)
  if not index_name:
    return None
  index_type = get_type(model)

  index_settings = copy.deepcopy(model._meta['elastic_search'])
  if 'river' in index_settings:
    if 'exclude_fields' not in index_settings['river'] or 'include_fields' not in index_settings['river']:
      index_settings['river']['exclude_fields'] = []
    index_settings['river'] = {
      'settings': index_settings['river'],
      'collection': model._get_collection_name()
    }
    if model._meta.get('allow_inheritance'):
      # Make sure we have _cls in index
      if '_cls' in index_settings['river'].get('exclude_fields', []):
        index_settings['river']['exclude_fields'].remove('_cls')
      elif index_settings['river'].get('include_fields') and '_cls' not in index_settings['river']['include_fields']:
        index_settings['river']['include_fields'].append('_cls')

  mappings = index_settings.get('mappings')
  if mappings and not isinstance(mappings, dict):
    raise TypeError("Specified es_mappings in '{}' is not a dict, it is '{}'".format(model.__name__,
                                                                                      type(fields).__name__))

  if mappings:
    # Get used analyzers
    analyzers = []
    fields = mappings.values()
    for field in fields:
      analyzers += [v for k, v in field.items() if k.startswith('analyzer')]
      for new_field in field.get('properties', {}).values():
        if isinstance(new_field, dict):
          fields.append(new_field)
    # Fetch custom analyzers from settings
    index_settings['analyzer'] = dict([(i, app.config['ES_ANALYZERS'][i]) for i in analyzers if i in app.config['ES_ANALYZERS']])

  if model._meta.get('allow_inheritance'):
    # Set (override) mapping for _cls
    index_settings.setdefault('mappings', {}).setdefault('properties', {})['_cls'] = {
      'type': 'string',
      'index': 'not_analyzed',
    }

  return index_name, index_type, index_settings

def prepare_indexes():
  from mongoengine import Document

  models = []
  all_models = set([Document])
  while all_models:
    model = all_models.pop()
    if not model._meta.get('abstract') and model._meta.get('elastic_search'):
      models.append(model)
    for model in model.__subclasses__():
      all_models.add(model)

  indexes = {}
  for model in models:
    index_name, index_type, index_settings = prepare_index(model)
    if index_name in indexes:
      indexes[index_name][index_type] = index_settings
    else:
      indexes[index_name] = {index_type: index_settings}
  return indexes

class ResultProxy(object):
  def __init__(self, model_cls, results):
    self._model_cls = model_cls
    self.es_results = results

  def _convert(self, obj):
    # There might be a better way...
    import json
    obj = self._model_cls.from_json(json.dumps(obj))
    return self._model_cls._from_son(obj.to_mongo())

  def __getitem__(self, i):
    if isinstance(i, slice):
      return [self._convert(j) for j in self.es_results[i]]
    else:
      return self._convert(self.es_results[i])

  def __len__(self):
    return len(self.es_results)

  def __iter__(self):
    for i in self.es_results:
      yield self._convert(i)

def _include_pyes(obj):
  for module in (pyes,):
    for key in module.__dict__:
      if not hasattr(obj, key):
        setattr(obj, key, getattr(module, key))

class PyESMongoEngine(object):
  def __init__(self, app = None):
    _include_pyes(self)

    if app is not None:
      self.init_app(app)

  def init_app(self, app):
    kwargs = dict([(k.lower(), v) for k, v in app.config.setdefault('ELASTICSEARCH_SETTINGS', {}).items()])
    self._index_settings = kwargs.pop('indices', {})
    self.conn = self.ES(**kwargs)

    self._mongodb_hosts = []
    for host in (i for i in app.config['MONGODB_SETTINGS']['HOST'].replace('mongodb://', '').split(',')):
      if ':' in host:
        host = host.split(':')
        self._mongodb_hosts.append({
          'host': host[0],
          'port': host[1]
        })
      else:
        self._mongodb_hosts.append({
          'host': host,
          'port': 27017
        })
    self._mongodb_db = app.config['MONGODB_SETTINGS']['DB']

    self._indexes_data = None

  @property
  def _indexes(self):
    if not self._indexes_data:
      self._indexes_data = prepare_indexes()
    return self._indexes_data

  def _get_river(self, model):
    if isinstance(model, tuple):
      index, type = model
    else:
      index = get_index_name(model)
      type = get_type(model)
    if not index:
      return None
    if type not in self._indexes[index]:
      return None

    river_properties = self._indexes[index][type].get('river', None)
    if not river_properties:
      return None

    river = pyes.rivers.MongoDBRiver(
      self._mongodb_hosts,
      self._mongodb_db,
      river_properties['collection'],
      index,
      type,
      options = river_properties['settings'].get('options'),
      script = river_properties['settings'].get('script')
    ), type
    return river

  def delete_river(self, model):
    river = self._get_river(model)
    if not river:
      return
    river, name = river
    try:
      # Remove river
      self.conn.delete_river(None, name)
    except (pyes.exceptions.TypeMissingException,\
            pyes.exceptions.IndexMissingException):
      pass

  def create_river(self, model):
    river = self._get_river(model)
    if not river:
      return
    river, name = river
    self.conn.create_river(river.serialize(), name)

  def delete_index(self, model):
    # Need to remove all rivers
    index = get_index_name(model)
    if not index:
      return
    for t in self._indexes[index].keys():
      self.delete_river((index, t))

    # We now remove the index itself
    try:
      # Remove index
      self.conn.indices.delete_index(index)
    except (pyes.exceptions.TypeMissingException,\
            pyes.exceptions.IndexMissingException):
      pass

  def create_index(self, model):
    index = get_index_name(model)
    if not index:
      return None

    # Get index settings
    settings = dict(self._index_settings.get('default', {}))
    settings.update(self._index_settings.get('collection', {}))
    # And put in the analyzers
    analyzers = {}
    for t in self._indexes[index].values():
      analyzers.update(t.get('analyzer', {}))
    if analyzers:
      settings['analyzer'] = analyzers

    # Put in all the mappings and flow in the rivers
    mappings = {}
    for t, s in self._indexes[index].items():
      if s.get('mappings'):
        mappings[t] = s['mappings']

    # Put mappings
    if mappings:
      settings['mappings'] = mappings
    # Create index
    self.conn.indices.create_index(index, settings)

    # Create Rivers
    for t in self._indexes[index].keys():
      self.create_river((index, t))

    # Done

  def recreate_index(self, model):
    # First, delete index and all it's rivers
    self.delete_index(model)
    # And create it
    self.create_index(model)

  def recreate_indexes(self):
    # Go through all indexes
    for index in self._indexes.keys():
      self.recreate_index(index)

  def search(self, indices, query, *args, **kwargs):
    """Searches in specified model's index.
    """
    index_name = get_index_name(indices)
    type = get_type(indices)

    from mongoengine import Document

    if issubclass(indices, Document) and indices._meta.get('allow_inheritance'):
      # We have models with inheritance, need to use _cls! Wrapping around a BoolQuery.
      query = pyes.BoolQuery(must = [query])
      query.add_must(pyes.PrefixQuery('_cls', indices._class_name))

    return ResultProxy(indices, self.conn.search(query, *args, doc_types = [type], indices = [index_name], **kwargs))
