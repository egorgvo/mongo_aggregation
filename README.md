# mongo_aggregation
Python MongoDB Aggregation ORM class.

### Installation

```bash
pip install mongo_aggregation
```

### Usage examples

```python
import pymongo
from datetime import datetime
from dateutil.relativedelta import relativedelta
from mongo_aggregation import MongoAggregation

# Usual pymongo connection
client = pymongo.MongoClient('mongodb://localhost:27017/test')
db = client.get_database()
today = datetime.now()
yesterday = today - relativedelta(days=1)

# Compose the pipeline
pipeline = MongoAggregation(collection=db.action)
pipeline.match(
    {'date': {'$gte': yesterday, '$lt': today}},
    completed=True,
    _cls={'$in': [
        'Action.Order', 'Action.StorageIncome', 'Action.StorageCancellation', 'Action.StorageMovementOutcome'
    ]},
).smart_project(
    'transactions.amount,_cls', '_id'
).append([
    {'$project': {
        '_cls': 1,
        'date': 1,
        'transactions.amount': 1,
        'transactions.cashbox': 1,
    }},
]).project(
    {'transactions.amount': 1, 'transactions.cashbox': 1}, _cls=1, date=1
).project(
    transactions=1, _cls=1, date=1
)
# Run it
cursor = pipeline.aggregate()

# Iterate over result
for doc in cursor:
    print(doc)
    break
```

#### Methods description

- [$sort](https://docs.mongodb.com/manual/reference/operator/aggregation/sort/)
```python
pipeline.sort('-doctor')
# {'$sort': {'doctor': -1}}

pipeline.sort({'doctor': 1})
# {'$sort': {'doctor': 1}}

pipeline.sort('doctor', '-patient')
# [{'$sort': {'doctor': 1}}, {'$sort': {'-patient': 1}}]
```

- `order_by`
is an alias to `sort`.
```python
pipeline.order_by('-doctor')
# {'$sort': {'doctor': -1}}
```

- [$replaceRoot](https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/)
```python
pipeline.replace_root('doctor')
# {'$replaceRoot': {'newRoot': '$doctor'}}
```

- [$count](https://docs.mongodb.com/manual/reference/operator/aggregation/count/)

Returns count. Does not add stage.
```python
pipeline.count()
2
```

#### Response as list

By default aggregate returns cursor. If you want it to return a list of documents use `as_list` argument:
```
data = pipeline.aggregate(as_list=True)
```

### Patterns module

Provides operators and some other patterns in python functions way.

Imports:
```python
from mongo_aggregation.patterns import regex
```

- [$and](https://docs.mongodb.com/manual/reference/operator/query/and/)
```python
and_({'a': True}, b=True, c=True)
# {'$and': [{'a': True}, {'b': True}, {'c': True}]}
```

- [$or](https://docs.mongodb.com/manual/reference/operator/query/or/)
```python
or_({'a': True}, b=True, c=True)
# {'$or': [{'a': True}, {'b': True}, {'c': True}]}
```

- [$regex](https://docs.mongodb.com/manual/reference/operator/query/regex/)
```python
regex('name', i=True)
# {'$regex': 'name', '$options': 'i'}
```

#### Aggregation patterns module
Provides aggregation operators and some other patterns in python functions way.

Imports:
```python
from mongo_aggregation.aggr_patterns import merge_objects
```

- [$ifNull](https://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/)  
There are two variations:
```python
if_null('$doctor', {})
{'$ifNull': ['$doctor', {}]}

ifNull('$doctor', '')
{'$ifNull': ['$doctor', '']}
```

Function looks after dollar prefix, so you can skip it: 
```python
if_null('doctor', {})
{'$ifNull': ['$doctor', {}]}
```

- [$in](https://docs.mongodb.com/manual/reference/operator/aggregation/in/)  
There are two variations:
```python
in_('doctor', 'doctors')
{'$in': ['$doctor', '$doctors']}
```

- [$mergeObjects](https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/)
```python
merge_objects('$doctor', first_name='John', last_name='Doe')
# {'$mergeObjects': ['$doctor', {'first_name': 'John', 'last_name': 'Doe'}]}
```

#### Others

- `obj` function  
Returns dictionary with specified fields as keys and values. For better understanding see usage examples below.

```python
>>> obj('name,description')
{'name': '$name', 'description': '$description'}

>>> obj('name', 'description')
{'name': '$name', 'description': '$description'}

>>> obj('name,description', id='$code')
{'name': '$name', 'description': '$description', 'id': '$code'} 
 ```

### Changelog

#### 1.0.8 (2020-12-24)

- Added `in_` aggregation pattern.

#### 1.0.8 (2020-12-11)

- Added `and_`, `and_nor`, `and_or` methods to `MongoMatchFilter`.

#### 1.0.7 (2020-09-19)

- Small fix.

#### 1.0.6 (2020-11-19)

- Added `if_null` pattern.
- Removed `get_count` method. `count` now returns count and does not add `count` stage.
- Added `obj` pattern. 

#### 1.0.5 (2020-09-25)

- Added `order_by` method.

#### 1.0.4 (2020-09-25)

- Bug fixes.

#### 1.0.3 (2020-09-01)

- `.sort` now supports `-email` sorting format (direction and field name by string).
- Added `.replace_root` method.
- Added `or_` and `and_` logical operators. `_and` is deprecated now.
- Added `regex` operator pattern.
- Added `merge_objects` aggregation operator (`./aggr_patterns`).

#### 1.0.2 (2020-09-01)

- Added `$addFields` and `$set` stages.
- Added `$filter` operator.