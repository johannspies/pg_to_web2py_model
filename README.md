# pg_to_web2py_model

A script that queries a PostgreSQL database and creates a model file for existing tables
Requires at least python3.6
This script expect the following to be in a model where this output will be used:

```from gluon.dal import SQLCustomType
tsv = SQLCustomType(
    type ='text',
    native='tsvector' )
citext = SQLCustomType(
    type ='text',
    native='citext' )
uuidtype = SQLCustomType(
    type = 'text',
    native = 'uuid'
)
boolean = SQLCustomType(
    type = 'boolean',
    native = 'boolean'
)
```
It takes into account cases where table names and field names might be *reserved words*.  
In such cases a "c_" will be added to the name and *rname*  will be used.

Typical web2py-auth-related tables like auth_user, auth_group etc.  are ignored.
And so all id-fields.

This version of the script expect a config file with the name "~/.pg.ini"
something like this:

```[DEFAULT]
host = localhost
user = thisuser
password = thisuserpassword
port = 5432
```

