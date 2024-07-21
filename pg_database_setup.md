# Setting Up PostgreSQL Database

Follow these steps to create a new PostgreSQL database instance, and the needed tables:
-------

1. Open your terminal.
2. Connect to PostgreSQL Server

----------------------------------------
> [!tip] 
>  Connecting to a PostgreSQL database can be done using various tools and methods depending on your environment and preferences:
>  - **Using psql (PostgreSQL Command Line Tool)**
>     - psql is a terminal-based front-end to PostgreSQL. To connect to a PostgreSQL database, use the following command:
>       ```shell
>         psql -h <host> -U <username> -d <database_name>
>       ```
>       - -h specifies the host.
>       - -U specifies the username.
>       - -d specifies the database name.
>         
>  - **Using a GUI Tool**
>     - eg: DataGrip (not free), Postici (for MacOS and not very powerful), pgAdmin (for Linux and windows)
>  - **Using Python (with psycopg2 library)**
>
> In this project, I'll be using psql and psycopg2
----------------------

> [!tip]
> - if your PostgreSQL server is running locally with default configurations, you can run the PostgreSQL shell by typing:
>   - **`sudo -u postgres`**:
>      - <ins>Purpose:</ins> Runs a single command as the postgres user without switching to an interactive shell.
>      - <ins>Usage:</ins> You specify the command you want to run immediately after the sudo -u postgres command.
>      - <ins>Effect:</ins> Only the specified command runs with postgres user privileges.
>      - <ins>Example:</ins> `sudo -u postgres psql`
>   - **`sudo su - postgres`**:
>      - <ins>Purpose:</ins> Switches to the postgres user and starts an interactive shell session.
>      - <ins>Usage:</ins> You switch to the postgres user and then you can run multiple commands interactively in a shell.
>      - <ins>Effect:</ins> You get a shell prompt as the postgres user, and you can run any number of commands without needing to prefix them with sudo.
>		
> - if your server is remote, or if you have specific configurations, you might need to provide those details explicitly in the command:
>    - <ins>Usage:</ins> Use the `psql` command with the appropriate options to specify the host, port, user, and database.
>      ```bash
>         psql -h your_host -p your_port -U your_username -d your_database
>      ```
>   - <ins>Example:</ins>
>      ```bash
>         psql -h 192.168.1.100 -p 5432 -U myuser -d mydatabase
>      ```

----------------------

==> Choose the method that is most convenient for you from the tips provided.

I used the following because I need to work with SQL queries:
```bash
sudo -u postgres psql
```

3. Inside the PostgreSQL shell, create a New Database:
```sql
CREATE DATABASE recipe_etl;
```

4. Switch to the newly created database:
```sql
\c recipe_etl;
```

5. Create tables according to the logical model
```sql
CREATE TABLE Recipe (
   id_recipe INT,
   recipe_title VARCHAR(100) NOT NULL,
   ready_min INT,
   summary VARCHAR(5000),
   servings INT,
   is_cheap BOOLEAN,
   price_per_serving DOUBLE PRECISION,
   is_vegetarian BOOLEAN,
   is_vegan BOOLEAN,
   is_glutenFree BOOLEAN,
   is_dairyFree BOOLEAN,
   is_healthy BOOLEAN,
   is_sustainable BOOLEAN,
   is_lowFodmap BOOLEAN,
   is_Popular BOOLEAN,
   license VARCHAR(100),
   source_url VARCHAR(500),
   PRIMARY KEY(id_recipe),
   UNIQUE(recipe_title)
);
```

```sql
CREATE TABLE Instruction(
   id_instruction INT,
   id_recipe INT NOT NULL,
   PRIMARY KEY(id_instruction),
   FOREIGN KEY(id_recipe) REFERENCES Recipe(id_recipe)
);
```

```sql
CREATE TABLE Ingredient(
   id_ingredient INT,
   ing_name VARCHAR(100) NOT NULL,
   consistency VARCHAR(100),
   aisle VARCHAR(100),
   PRIMARY KEY(id_ingredient)
);
```

```sql
CREATE TABLE Step(
   id_step INT,
   step VARCHAR(10000) NOT NULL,
   number INT,
   length VARCHAR(50),
   id_instruction INT NOT NULL,
   PRIMARY KEY(id_step),
   FOREIGN KEY(id_instruction) REFERENCES Instruction(id_instruction)
);
```

```sql
CREATE TABLE Equipment(
   id_equipment INT,
   equip_name VARCHAR(500) NOT NULL,
   PRIMARY KEY(id_equipment),
   UNIQUE(equip_name)
);
```

```sql
CREATE TABLE dish(
   id_dish_type INT,
   dish_type VARCHAR(500) NOT NULL,
   PRIMARY KEY(id_dish_type),
   UNIQUE(dish_type)
);
```

```sql
CREATE TABLE Cuisine(
   id_cuisine INT,
   recipe_cuisine VARCHAR(500) NOT NULL,
   PRIMARY KEY(id_cuisine),
   UNIQUE(recipe_cuisine)
);
```

```sql
CREATE TABLE reference_ing(
   id_recipe INT,
   id_ingredient INT,
   measure VARCHAR(50),
   FOREIGN KEY(id_recipe) REFERENCES Recipe(id_recipe),
   FOREIGN KEY(id_ingredient) REFERENCES Ingredient(id_ingredient)
);
```

```sql
CREATE TABLE reference_equip(
   id_recipe INT,
   id_step INT,
   id_equipment INT,
   FOREIGN KEY(id_recipe) REFERENCES Recipe(id_recipe),
   FOREIGN KEY(id_step) REFERENCES Step(id_step),
   FOREIGN KEY(id_equipment) REFERENCES Equipment(id_equipment)
);
```

```sql
CREATE TABLE is_a(
   id_recipe INT,
   id_dish_type INT,
   FOREIGN KEY(id_recipe) REFERENCES Recipe(id_recipe),
   FOREIGN KEY(id_dish_type) REFERENCES dish(id_dish_type)
);
```

```sql
CREATE TABLE belongs(
   id_recipe INT,
   id_cuisine INT,
   FOREIGN KEY(id_recipe) REFERENCES Recipe(id_recipe),
   FOREIGN KEY(id_cuisine) REFERENCES Cuisine(id_cuisine)
);
```
6. To display the list of tables in your current PostgreSQL database, you can use:
```sql
\dt
````
7. Create a new user with a password and grant necessary privileges:
```sql
CREATE USER maryem WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE your_db_name TO your_username;

GRANT ALL PRIVILEGES ON TABLE public.belongs TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.cuisine TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.dish TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.equipment TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.ingredient TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.instruction TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.is_a TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.recipe TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.reference_equip TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.reference_ing TO your_username;
GRANT ALL PRIVILEGES ON TABLE public.step TO your_username;
```

8. Update Python Script with Connection Details
```python
# Replace these with your PostgreSQL connection details
DB_HOST = ' x.x.x.x'  # your server IP
DB_PORT = '5432'  # or your PostgreSQL port
DB_NAME = 'recipe_etl'
DB_USER = 'maryem' #you can use the postgres user 
DB_PASSWORD = 'your_password'
```
----------------
> [!tip]
> If you encounter errors when executing the connection section in your python code follow thse steps:
> 1. connect to postgresql:
> ```bash
> sudo su - postgres
> ```
> 2. Locate and edit the pg_hba.conf file on your PostgreSQL server:
> ```bash
> updatedb
> locate pg_hba.conf
> nano /etc/postgresql/14/main/pg_hba.conf
> ```
> 3. Add an entry to allow the desired connection. For example, to allow the user postgres to connect to the recipe_etl database from the host x.x.x.x:
> ```
> host    recipe_etl    postgres    x.x.x.x/32    md5
> ```
> After editing the pg_hba.conf file, you need to restart the PostgreSQL configuration to apply the changes. You can do this by running the following command:
> ```bash
> exit
> sudo systemctl restart postgresql
> sudo systemctl status postgresql
> ```

------------------

> [!tip]
> If you want to delete all the data in your tables, use the following SQL syntax:
> ```SQL
> -- Start by deleting rows from the tables that have no dependencies on other tables
>
> -- Step 1: Delete from child tables
> DELETE FROM reference_equip;
> DELETE FROM reference_ing;
> DELETE FROM step;
>
> -- Step 2: Delete from parent tables
> DELETE FROM instruction;
> DELETE FROM belongs;
> DELETE FROM is_a;
> DELETE FROM cuisine;
> DELETE FROM dish;
> DELETE FROM equipment;
> DELETE FROM ingredient;
> DELETE FROM recipe;
> ```

---------------------
