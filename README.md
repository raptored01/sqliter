# SQLiter
**[WORK IN PROGRESS]**

A simple wrapper for Python's SQLite3

## Known issues:
- the **default** keyword in Fields may be vulnerable to SQL Injection
- the **in** lookup doesn't work

## Major missing features:
- table altering
- unique fields and primary keys cannot come in a pair or more (equivalent of UNIQUE (field1, field2))
- foreign keys and unique fields are declared inline and insted of at the bottom of the CREATE statement
- automatic primary key adding when not provided (?)
- automatic timestamping (*CURRENT_TIMESTAMP*)
- complex lookups (combinations of *ors* & *ands*)

## Example usage:
### import the classes
    from sqliter import Database, Fields
    import datetime
    import random
    
    # either connect to or create a database
    database = Database("test.db")
    
### create a table
    database.create_table(
        "persons",
        id=Fields.Integer(pk=True),
        name=Fields.Text(null=False),
        city=Fields.Text(null=False, default="London"),
        birthday=Fields.Date(),
    )
    # also available: .create_table_if_not_exists()
    
### get a table
    persons = database.table("persons")
    
### create and get a table
    dogs = database.create_table(
        "dogs",
        id=Fields.Integer(pk=True),
        name=Fields.Text(null=False),
        age=Fields.Integer(null=False),
        owner=Fields.ForeignKey(persons, on_delete=Fields.CASCADE)
    )
    
### create a new entry
    persons.create(name="Sally", birthday=datetime.date(1995, 1, 11))
    # also available: .create_or_replace()
    # also available: .create_or_ignore()
    
### get an entry
    sally = persons.get(name="Sally")
    
### create and get an entry
    bob = persons.create(name="Bob", birthday=datetime.date(1990, 10, 10))
    frank = persons.create(name="Frank", birthday=datetime.date(1985, 6, 1))
    alice = persons.create(name="Alice", birthday=datetime.date(1995, 4, 22))
    anna = persons.create(name="Anna", birthday=datetime.date(1970, 5, 12))
    
### create multiple entries at once
    dog_names = ["Max", "Charlie", "Bella", "Lucy", "Molly", "Rocky"]
    dogs.bulk_create(
        dict(
            name=name,
            age=random.randint(1, 10),
            owner=random.choice([bob, frank, alice, anna, sally])
        ) for name in dog_names
    )
    
### reference a related instance
    maxs_owner = dogs.get(name="Max").owner
    
### update an entry
    maxs_owner.name = "Max's Owner"
    maxs_owner.save()
    
### delete an entry
    # will delete also its dog since on_delete=Fields.CASCADE
    maxs_owner.delete()
    
### filter the entries and get a QuerySet
    dogs_older_than_five = dogs.filter(age__gt=5)
    
### loop through the QuerySet
    for dog in dogs.filter(id__lte=5).order_by("-age"):
        print(dog)
    
### update a QuerySet
    dogs_older_than_five.update(name="Old dog")
    
### delete a QuerySet
    dogs.filter(name__icontains="y").delete()
    
### drop a table
    dogs.drop()
    
### clear a table
    persons.clear()
