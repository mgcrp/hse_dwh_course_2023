### План семинара

1) Активируем БД
`sh demo_db/docker-init.sh`

2) Активируем Airflow
`cd demo_airflow && docker-compose up -d`

3) Добавим connection
host: host.docker.internal
port: 5432
user: postgres
pass: postgres
db: postgres

4) Давайте напишем простой DAG с sqlOperator
demo_airflow/dags/dag_demoSql.py
Сначала убрать .strptime , чтобы показать, как работают ошибки
Потом добавить и показать, что все отработало
Показать https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

5) Показать DAG с sqlSensor
demo_airflow/dags/dag_demoSqlSensor.py

6) Показать DAG с pythonOperator
 - Кладем токен в секреты
   host: https://api.telegram.org/bot
   user: hse_dwh_course_bot
   pass: 6777899526:AAF1QjMBKHYfQmg-VXh9IsTgtuQ105d4OYw
 - demo_airflow/dags/dag_demoPython.py

7) Показать OnFailureCallback
demo_airflow/dags/dag_demoOnFailureCallback.py

---

1) Ставим dbt
pip3 install dbt
pip3 install dbt-postgres

2) Создаем проект
dbt init demo_dbt

3) Настраиваем profiles.yml
Показываем https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup

4) Настраиваем dbt_project.yml
project postgres
+schema: dbt_example

5) Проверяем подключение через dbt debug

6) Смотрим на стандартные примеры - dbt run

7) Добавим в систему source - сырые таблицы
cd models
mkdir raw
cd raw
touch schema.yml
```
version: 2

sources:
  - name: postgres_business
    description: 'blah-blah-blah'
    database: postgres
    schema: business
    tables:
      - name: categories
        description: 'A table containing item categories'
        columns:
          - name: category_id
            description: 'INT, primary key'
          - name: ategory_name
            description: 'TEXT, human readable name'
      - name: customers
      - name: manufacturers
      - name: deliveries
      - name: price_change
      - name: products
      - name: purchase_items
      - name: purchases
      - name: stores
```
dbt run

8) /target/compiled - расписанное в .sql без jinja

9) Нам не нравится, что оно пишет в кривую схему
    - на уровне таблицы
      {{ config(materialized='table', schema='staging') }}
    - на уровне проекта (dbt_project.yml)
      +schema: staging
    - изобретаем макрос
        - показываем https://docs.getdbt.com/docs/build/custom-schemas
        - touch /macros/generate_schema_name.sql
        - ```
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
        ```
dbt run

10) Создадим новую таблицу
    - cd /models
    - mkdir presentation
    - touch presentation__gmv_by_store_category.sql
    - ```
with final as (
    SELECT
        pu.store_id,
        pr.category_id,
        SUM(pi.product_price * pi.product_count) AS sales_sum
    FROM
        {{ source('postgres_business', 'purchase_items') }} pi
    JOIN
        {{ source('postgres_business', 'products') }} pr ON pi.product_id = pr.product_id
    JOIN
        {{ source('postgres_business', 'purchases') }} pu ON pi.purchase_id = pu.purchase_id
    GROUP BY
        pu.store_id,
        pr.category_id
)

select * from final
    ```
    - обновляем dbt_project.yml
    ```
    presentation:
      +materialized: table
      +schema: dbt_presentation
    ```
    - dbt run

11) dbt test
Тесты пишутся в schema.yml
Смотрим на тесты на примере
Скомпилированные тесты пишутся в /target/compiled/.../models/example/schema.yml

12) Материализации
    - table
    - view
    - incremental
    - ephemeral
Дефолтный - view
Можно на уровне проекта, можно в каждой таблице отдельно

13) seeds
статическая инфа, которая меняется редко
кладется в формате csv в seeds
    - cd seeds
    - touch dict_cities.csv
    - ```
id, name, code
1, Moscow, MSK
2, Saint-Peterburg, SPB
3, Ekaterinburg, EKB
4, Novosibirsk, NSK
    ```
    - dbt seed

14) Автодока
dbt docs generate
dbt docs serve --port 1111

15) существуют дофига packages
    - показать https://hub.getdbt.com
    - создать в проекте packages.yml
    - прописать
    - dbt deps
    - лежит в dbt_packages

    - touch packages.yml
    - ```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
    ```
    - dbt deps

16) свой тест
test_is_moscow.sql
```
{% macro test_is_moscow(model, column_name) %}

with validation as (
    select {{ column_name }} as ts
    from {{ model }}
),
validation_errors as (
    select ts
    from validation
    where 1=1
        and ts < '2023-01-01'
)
select count(*)
from validation_errors
```

17) свой ref

---

dbt vault

dbt run -m tag:raw
dbt run -m tag:stage
dbt run -m tag:hub
dbt run -m tag:link
dbt run -m tag:satellite
dbt run -m tag:t_link

# dbt_profiles.yml

vars:
  load_date: '1992-01-08' # increment by one day '1992-01-09'