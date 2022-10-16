from dagster import asset
from jaffle.duckpond import SQL
import pandas as pd


@asset
def cereals() -> SQL:
    return SQL(
        "select * from $df", df=pd.read_csv("https://docs.dagster.io/assets/cereal.csv")
    )


@asset
def mfg_popularity(cereals: SQL) -> SQL:
    return SQL(
        "select mfr, count(*) as num_cereals from $cereals group by 1 order by 2 desc",
        cereals=cereals,
    )


@asset(required_resource_keys={"duckdb"})
def print_mfg_popularity(context, mfg_popularity: SQL):
    context.log.info(f"Final asset:")
    context.log.info(context.resources.duckdb.query(mfg_popularity))


@asset
def stg_customers() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
    )
    df.rename(columns={"id": "customer_id"}, inplace=True)
    return SQL("select * from $df", df=df)


@asset
def stg_orders() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    )
    df.rename(columns={"id": "order_id", "user_id": "customer_id"}, inplace=True)
    return SQL("select * from $df", df=df)


@asset
def stg_payments() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"
    )
    df.rename(columns={"id": "payment_id"}, inplace=True)
    df["amount"] = df["amount"].map(lambda amount: amount / 100)
    return SQL("select * from $df", df=df)


@asset
def customers(stg_customers: SQL, stg_orders: SQL, stg_payments: SQL) -> SQL:
    return SQL(
        """
with customers as (
    select * from $stg_customers
),
orders as (
    select * from $stg_orders
),
payments as (
    select * from $stg_payments
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),
customer_payments as (
    select
        orders.customer_id,
        sum(amount) as total_amount
    from payments
    left join orders on
         payments.order_id = orders.order_id
    group by orders.customer_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value
    from customers
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id
    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id
)

select * from final
    """,
        stg_customers=stg_customers,
        stg_orders=stg_orders,
        stg_payments=stg_payments,
    )


@asset
def orders(stg_orders: SQL, stg_payments: SQL) -> SQL:
    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]
    return SQL(
        f"""
with orders as (
    select * from $stg_orders
),
payments as (
    select * from $stg_payments
),
order_payments as (
    select
        order_id,
        {"".join(f"sum(case when payment_method = '{payment_method}' then amount else 0 end) as {payment_method}_amount," for payment_method in payment_methods)}
        sum(amount) as total_amount
    from payments
    group by order_id
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {"".join(f"order_payments.{payment_method}_amount," for payment_method in payment_methods)}

        order_payments.total_amount as amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
)

select * from final
    """,
        stg_orders=stg_orders,
        stg_payments=stg_payments,
    )


@asset(required_resource_keys={"duckdb"})
def preview_all(context, customers: SQL, orders: SQL):
    context.log.info(f"Customers:")
    context.log.info(context.resources.duckdb.query(customers))
    context.log.info(f"Orders:")
    context.log.info(context.resources.duckdb.query(orders))
