from __future__ import absolute_import, print_function, division
from pony.py23compat import int_types, basestring, imap, iteritems

import json
from operator import attrgetter
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

from pony.orm.core import Attribute, Set, Entity, EntityMeta, TransactionError, db_session, flush
                          # PermissionError, get_current_user, get_current_user_groups
                          # can_view, can_edit, can_delete
from pony.utils import throw, cut_traceback

__all__ = 'basic_converter', 'get_schema_dict', 'get_schema_json', 'to_json', 'save_changes'

def basic_converter(x):
    if isinstance(x, (datetime, date, Decimal)):
        return str(x)
    if isinstance(x, dict):
        return dict(x)
    if isinstance(x, Entity):
        pkval = x._get_raw_pkval_()
        return pkval[0] if len(pkval) == 1 else pkval
    try: iter(x)
    except: raise TypeError(x)
    return list(x)

def get_schema_dict(db):
    result = []
    for entity in sorted(db.entities.values(), key=attrgetter('_id_')):
        # if not can_view(entity): continue
        attrs = []
        for attr in entity._new_attrs_:
            d = dict(name = attr.name, type = attr.py_type.__name__, kind = attr.__class__.__name__)
            if attr.auto: d['auto'] = True
            if attr.reverse:
                # if not can_view(attr.reverse.entity): continue
                d['reverse'] = attr.reverse.name
            if attr.lazy: d['lazy'] = True
            if attr.nullable: d['nullable'] = True
            if attr.default and issubclass(type(attr.default), (int_types, basestring)):
                d['defaultValue'] = attr.default
            attrs.append(d)
        d = dict(name=entity.__name__, newAttrs=attrs, pkAttrs=[ attr.name for attr in entity._pk_attrs_ ])
        if entity._all_bases_:
            d['bases'] = [ base.__name__ for base in entity._all_bases_ ]
        if entity._simple_keys_:
            d['simpleKeys'] = [ attr.name for attr in entity._simple_keys_ ]
        if entity._composite_keys_:
            d['compositeKeys'] = [ [ attr.name for attr in attrs ] for attrs in entity._composite_keys_ ]
        result.append(d)
    return result

def get_schema_json(db):
    return json.dumps(get_schema_dict(db), default=basic_converter)

@cut_traceback
def to_json(database, data, include=(), exclude=(), converter=None, with_schema=True):
    for attrs, param_name in ((include, 'include'), (exclude, 'exclude')):
        for attr in attrs:
            if not isinstance(attr, Attribute): throw(TypeError,
                "Each item of '%s' list should be attribute. Got: %s" % (param_name, attr))
    include, exclude = set(include), set(exclude)
    if converter is None: converter = basic_converter

    # def user_has_no_rights_to_see(obj, attr=None):
    #     user_groups = get_current_user_groups()
    #     throw(PermissionError, 'The current user %s which belongs to groups %s '
    #                            'has no rights to see the object %s on the frontend'
    #                            % (get_current_user(), sorted(user_groups), obj))

    object_set = set()
    caches = set()
    def obj_converter(obj):
        if not isinstance(obj, Entity): return converter(obj)
        caches.add(obj._session_cache_)
        if len(caches) > 1: throw(TransactionError,
            'An attempt to serialize objects belonging to different transactions')
        # if not can_view(obj):
        #     user_has_no_rights_to_see(obj)
        object_set.add(obj)
        pkval = obj._get_raw_pkval_()
        if len(pkval) == 1: pkval = pkval[0]
        return { 'class': obj.__class__.__name__, 'pk': pkval }
                
    data_json = json.dumps(data, default=obj_converter)

    objects = {}
    if caches:
        cache = caches.pop()
        if cache.database is not database:
            throw(TransactionError, 'An object does not belong to specified database')
        object_list = list(object_set)
        objects = {}
        for obj in object_list:
            if obj in cache.seeds[obj._pk_attrs_]: obj._load_()
            entity = obj.__class__
            # if not can_view(obj):
            #     user_has_no_rights_to_see(obj)
            d = objects.setdefault(entity.__name__, {})
            for val in obj._get_raw_pkval_(): d = d.setdefault(val, {})
            assert not d, d
            for attr in obj._attrs_:
                if attr in exclude: continue
                if attr in include: pass
                    # if attr not in entity_perms.can_read: user_has_no_rights_to_see(obj, attr)
                elif attr.is_collection: continue
                elif attr.lazy: continue
                # elif attr not in entity_perms.can_read: continue

                if attr.is_collection:
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    value = []
                    for item in attr.__get__(obj):
                        if item not in object_set:
                            object_set.add(item)
                            object_list.append(item)
                        pkval = item._get_raw_pkval_()
                        value.append(pkval[0] if len(pkval) == 1 else pkval)
                    value.sort()
                else:
                    value = attr.__get__(obj)
                    if value is not None and attr.is_relation:
                        if attr in include and value not in object_set:
                            object_set.add(value)
                            object_list.append(value)
                        pkval = value._get_raw_pkval_()
                        value = pkval[0] if len(pkval) == 1 else pkval

                d[attr.name] = value
    objects_json = json.dumps(objects, default=converter)
    if not with_schema:
        return '{"data": %s, "objects": %s}' % (data_json, objects_json)
    schema_json = get_schema_json(database)
    return '{"data": %s, "objects": %s, "schema": %s}' % (data_json, objects_json, schema_json)

@cut_traceback
@db_session
def save_changes(db, changes, observer=None):
    changes = json.loads(changes)

    import pprint; pprint.pprint(changes)

    objmap = {}
    for diff in changes['objects']:
        if diff['_status_'] == 'c': continue
        pk = diff['_pk_']
        pk = (pk,) if type(pk) is not list else tuple(pk)
        entity_name = diff['class']
        entity = db.entities[entity_name]
        obj = entity._get_by_raw_pkval_(pk, from_db=False)
        oid = diff['_id_']
        objmap[oid] = obj

    def id2obj(attr, val):
        return objmap[val] if attr.reverse and val is not None else val

    # def user_has_no_rights_to(operation, obj):
    #     user_groups = get_current_user_groups()
    #     throw(PermissionError, 'The current user %s which belongs to groups %s '
    #                            'has no rights to %s the object %s on the frontend'
    #                            % (get_current_user(), sorted(user_groups), operation, obj))

    for diff in changes['objects']:
        entity_name = diff['class']
        entity = db.entities[entity_name]
        dbvals = {}
        newvals = {}
        for name, val in diff.items():
            if name not in ('class', '_pk_', '_id_', '_status_'):
                attr = entity._adict_[name]
                if not attr.is_collection:
                    if type(val) is dict:
                        if 'old' in val: dbvals[attr] = attr.validate(id2obj(attr, val['old']))
                        if 'new' in val: newvals[attr.name] = attr.validate(id2obj(attr, val['new']))
                    else: newvals[attr.name] = attr.validate(id2obj(attr, val))
        oid = diff['_id_']
        status = diff['_status_']
        if status == 'c':
            assert not dbvals
            obj = entity(**newvals)
            if observer:
                flush()  # in order to get obj.id
                observer('create', obj, newvals)
            objmap[oid] = obj
            # if not can_edit(obj): user_has_no_rights_to('create', obj)
        else:
            obj = objmap[oid]
            if status == 'd':
                # if not can_delete(obj): user_has_no_rights_to('delete', obj)
                if observer: observer('delete', obj)
                obj.delete()
            elif status == 'u':
                # if not can_edit(obj): user_has_no_rights_to('update', obj)
                if newvals:
                    assert dbvals
                    if observer:
                        oldvals = dict((attr.name, val) for attr, val in iteritems(dbvals))
                        observer('update', obj, newvals, oldvals)
                    obj._db_set_(dbvals)  # dbvals can be modified here
                    for attr in dbvals: attr.__get__(obj)
                    obj.set(**newvals)
                else: assert not dbvals
                objmap[oid] = obj
    flush()
    for diff in changes['objects']:
        if diff['_status_'] == 'd': continue
        obj = objmap[diff['_id_']]
        entity = obj.__class__
        for name, val in diff.items():
            if name not in ('class', '_pk_', '_id_', '_status_'):
                attr = entity._adict_[name]
                if attr.is_collection and attr.reverse.is_collection and attr < attr.reverse:
                    removed = [ objmap[oid] for oid in val.get('removed', ()) ]
                    added = [ objmap[oid] for oid in val.get('added', ()) ]
                    collection = attr.__get__(obj)
                    if removed:
                        observer('remove', obj, {name: removed})
                        collection.remove(removed)
                    if added:
                        observer('add', obj, {name: added})
                        collection.add(added)
    flush()

    def deserialize(x):
        t = type(x)
        if t is list: return list(imap(deserialize, x))
        if t is dict:
            if '_id_' not in x:
                return dict((key, deserialize(val)) for key, val in iteritems(x))
            obj = objmap.get(x['_id_'])
            if obj is None:
                entity_name = x['class']
                entity = db.entities[entity_name]
                pk = x['_pk_']
                obj = entity[pk]
            return obj
        return x

    return deserialize(changes['data'])

