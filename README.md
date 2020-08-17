# mongo_aggregation
Python MongoDB Aggregation ORM class.

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

#### Response as list

By default aggregate returns cursor. If you want it to return a list of documents use `as_list` argument:
```
cursor = pipeline.aggregate(as_list=True)
```
