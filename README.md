# Welcome to my challenge solution

-------------------
Name: Sebastian Sanchez Bernal

-------------------

## Some notes:
To solve this porblem I used postgreSQL in Heroku, with the python library `sqlalchemy`. In order to run my code I set a `.env` file with a link (heroku URI) that looks like: 

```{bash}
DATABASE_URL=postgres://<username>:<password>@<hostname>:5432/<database_name>
```

The file that I ran in order to perform all the operations was `main.py`.

The other two python files (`data_ingest.py` and `data_structure.py`) contain the classes that perform all the operations in order to clean the data. Finally I prepared a `.conf` file to run PostgREST locally. The strucutre of this file is: 

```{.conf}
db-uri = "postgres://<username>:<password>@<hostname>:5432/<database_name>"
db-schema = "public"  
db-anon-role = "<user>"
```
Both `.env` and `.conf` files are not in the repository. 


## Results

My results have the next structure when I called the rest api:
```
{"date":"September 2020","valueInThousands":544.5}, 
 {"date":"October 2020","valueInThousands":544.1}, 
 {"date":"November 2020","valueInThousands":542.2}, 
 {"date":"December 2020","valueInThousands":534.9}, 
 {"date":"January 2021","valueInThousands":532.6}, 
 {"date":"February 2021","valueInThousands":540.6}, 
 {"date":"March 2021","valueInThousands":547.6}, 
 {"date":"April 2021","valueInThousands":553.8}, 
```



