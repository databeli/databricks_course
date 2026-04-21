-- Databricks notebook source
select
order_year,
measure(total_revenue),
measure(order_count)
from
metric_view_demo.my_schema.order_metrics
group by all

-- COMMAND ----------

select * from metric_view_demo.my_schema.order_metrics

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_sql
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year

measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
$$

-- COMMAND ----------

select
order_year,
measure(total_revenue),
measure(order_count)
from
metric_view_demo.my_schema.order_metrics_sql
group by all

-- COMMAND ----------

select
order_year,
measure(total_revenue),
measure(order_count),
measure(avg_revenue)
from
metric_view_demo.my_schema.order_metrics_sql
group by all

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_star
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year

measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
$$

-- COMMAND ----------

select
order_year,
measure(total_revenue),
measure(order_count),
measure(avg_revenue),
measure(total_quantity)
from
metric_view_demo.my_schema.order_metrics_star
group by all

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_star_2
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey
  - name: customer
    source: samples.tpch.customer
    on: source.o_custkey = customer.c_custkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year
  - name: customer_name
    expr: customer.c_name
    display_name: Customer Name


measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
$$

-- COMMAND ----------

select
order_year,
customer_name,
measure(total_revenue),
measure(order_count),
measure(avg_revenue),
measure(total_quantity)
from
metric_view_demo.my_schema.order_metrics_star_2
group by all

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_snowflake
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey
  - name: customer
    source: samples.tpch.customer
    on: source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        on: customer.c_nationkey = nation.n_nationkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year
  - name: customer_name
    expr: customer.c_name
    display_name: Customer Name

  - name: country
    expr: customer.nation.n_name
    display_name: Country


measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
$$

-- COMMAND ----------

select
country,
measure(total_revenue),
measure(order_count),
measure(avg_revenue),
measure(total_quantity)
from
metric_view_demo.my_schema.order_metrics_snowflake
group by all

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_snowflake
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey
  - name: customer
    source: samples.tpch.customer
    on: source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        on: customer.c_nationkey = nation.n_nationkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year
  - name: customer_name
    expr: customer.c_name
    display_name: Customer Name

  - name: country
    expr: customer.nation.n_name
    display_name: Country

  - name: order_status
    expr: |-
     case o_orderstatus
      when 'F' then 'Fulfilled'
      when 'O' then 'Open'
      when 'P' then 'Processing'
      end
    display_name: Order Status


measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
$$

-- COMMAND ----------

select
order_status,
measure(total_revenue),
measure(order_count),
measure(avg_revenue),
measure(total_quantity)
from
metric_view_demo.my_schema.order_metrics_snowflake
group by all

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_window
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey
  - name: customer
    source: samples.tpch.customer
    on: source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        on: customer.c_nationkey = nation.n_nationkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year
  - name: customer_name
    expr: customer.c_name
    display_name: Customer Name

  - name: country
    expr: customer.nation.n_name
    display_name: Country

  - name: order_status
    expr: |-
     case o_orderstatus
      when 'F' then 'Fulfilled'
      when 'O' then 'Open'
      when 'P' then 'Processing'
      end
    display_name: Order Status


measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
  - name: customers_last_7_days
    expr: count(distinct o_custkey)
    window: 
     - order: order_date
       semiadditive: last
       range: current
    display_name: Customers Last 7 Days

$$

-- COMMAND ----------

select
order_date,
measure(customers_last_7_days)
from
metric_view_demo.my_schema.order_metrics_window
group by all
order by order_date

-- COMMAND ----------

create or replace view metric_view_demo.my_schema.order_metrics_materilizaed
with metrics
language yaml
AS $$
version: 1.1

source: samples.tpch.orders

joins:
  - name: lineitem
    source: samples.tpch.lineitem
    on: source.o_orderkey = lineitem.l_orderkey
  - name: customer
    source: samples.tpch.customer
    on: source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        on: customer.c_nationkey = nation.n_nationkey

dimensions:
  - name: order_date
    expr: source.o_orderdate
    display_name: Order Date
    synonyms: ["Order Date", "Date of Order", "Purchase Date"]
  - name: order_year
    expr: YEAR(source.o_orderdate)
    display_name: Order Year
  - name: customer_name
    expr: customer.c_name
    display_name: Customer Name

  - name: country
    expr: customer.nation.n_name
    display_name: Country

  - name: order_status
    expr: |-
     case o_orderstatus
      when 'F' then 'Fulfilled'
      when 'O' then 'Open'
      when 'P' then 'Processing'
      end
    display_name: Order Status


measures:
  - name: total_revenue
    expr: SUM(source.o_totalprice)
    display_name: Total Revenue
    synonyms: ["Total Revenue", "Revenue", "Sales Amount"]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact

  - name: order_count
    expr: COUNT(*)
    display_name: Order Count

  - name: avg_revenue
    expr: MEASURE(total_revenue)/MEASURE(order_count)
    display_name: Average Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
  
  - name: total_quantity
    expr: sum(lineitem.l_quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false
      abbreviation: compact
  - name: customers_last_7_days
    expr: count(distinct o_custkey)
    window: 
     - order: order_date
       semiadditive: last
       range: current
    display_name: Customers Last 7 Days

materialization:
  schedule: every 6 hours
  mode: relaxed

  materialized_views:
    - name: baseline
      type: aggregated
      dimensions:
        - order_date
        - order_status

      measures:
       - total_revenue
       - order_count
$$

-- COMMAND ----------

select
order_date,
measure(customers_last_7_days)
from
metric_view_demo.my_schema.order_metrics_materilizaed
group by all
order by order_date

-- COMMAND ----------

describe extended metric_view_demo.my_schema.order_metrics_materilizaed