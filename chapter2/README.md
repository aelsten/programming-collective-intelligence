Using delicious.py to load data from downloaded data set:
```python
import delicious
del_users = delicious.get_user_dict_from_popular('programming')
```

Then things can be done as illustrated in the book:
```python
import random
user = del_users.keys()[random.randint(0, len(del_users)-1)]
import recommendations
recommendations.top_matches(del_users, user)
```
