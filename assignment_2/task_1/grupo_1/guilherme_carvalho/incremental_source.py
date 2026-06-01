#!/usr/bin/env python3
import argparse
import datetime
import os
import random
import sys

import pymysql
from pymysql.cursors import DictCursor

PIPELINE_NAME = "classicmodels_sales"
WATERMARK_TABLE = "etl_watermark"


def get_connection():
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME", "classicmodels")
    port = int(os.getenv("DB_PORT", "3306"))

    missing = [name for name, value in (
        ("DB_HOST", host),
        ("DB_USER", user),
        ("DB_PASSWORD", password),
    ) if not value]
    if missing:
        raise SystemExit(
            "Missing environment variables: {}.\n"
            "Set DB_HOST, DB_USER, DB_PASSWORD and optionally DB_NAME, DB_PORT.".format(
                ", ".join(missing)
            )
        )

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        cursorclass=DictCursor,
        autocommit=False,
    )


def create_watermark_table(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            pipeline_name VARCHAR(64) PRIMARY KEY,
            last_processed_order_date DATE,
            last_run_at DATETIME,
            last_run_status VARCHAR(32)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )


def get_orders_max_date(cursor):
    cursor.execute("SELECT MAX(orderDate) AS max_date FROM orders")
    row = cursor.fetchone()
    return row["max_date"] if row else None


def get_watermark(cursor):
    cursor.execute(
        f"SELECT pipeline_name, last_processed_order_date, last_run_at, last_run_status"
        f" FROM {WATERMARK_TABLE} WHERE pipeline_name = %s",
        (PIPELINE_NAME,),
    )
    return cursor.fetchone()


def init_watermark(args):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            create_watermark_table(cursor)

            current_max = get_orders_max_date(cursor)
            if current_max is None:
                raise SystemExit("orders table is empty or does not exist.")

            watermark = get_watermark(cursor)
            if watermark is None:
                cursor.execute(
                    f"INSERT INTO {WATERMARK_TABLE} "
                    "(pipeline_name, last_processed_order_date, last_run_at, last_run_status) "
                    "VALUES (%s, %s, NULL, 'NEVER_RUN')",
                    (PIPELINE_NAME, current_max),
                )
                conn.commit()
                print(f"Inserted watermark row for {PIPELINE_NAME} with last_processed_order_date={current_max}.")
            elif watermark["last_processed_order_date"] is None:
                cursor.execute(
                    f"UPDATE {WATERMARK_TABLE} SET last_processed_order_date = %s, last_run_status = 'NEVER_RUN' "
                    "WHERE pipeline_name = %s",
                    (current_max, PIPELINE_NAME),
                )
                conn.commit()
                print(f"Updated watermark row for {PIPELINE_NAME} with last_processed_order_date={current_max}.")
            else:
                print(
                    f"Watermark row already exists for {PIPELINE_NAME} "
                    f"with last_processed_order_date={watermark['last_processed_order_date']}.")


def choose_random_existing_items(cursor):
    cursor.execute("SELECT customerNumber FROM customers ORDER BY RAND() LIMIT 1")
    customer = cursor.fetchone()
    if not customer:
        raise SystemExit("No customers found in the database.")
    cursor.execute(
        "SELECT productCode, buyPrice FROM products ORDER BY RAND() LIMIT 1"
    )
    product = cursor.fetchone()
    if not product:
        raise SystemExit("No products found in the database.")
    return customer["customerNumber"], product["productCode"], float(product["buyPrice"])


def simulate_new_orders(args):
    random.seed(args.seed)
    with get_connection() as conn:
        with conn.cursor() as cursor:
            watermark = get_watermark(cursor)
            if watermark is None:
                raise SystemExit(
                    "Watermark record missing. Run the init_watermark command first."
                )
            base_watermark = watermark["last_processed_order_date"]
            max_order_date = get_orders_max_date(cursor)
            if max_order_date is None and base_watermark is None:
                base_date = datetime.date.today() - datetime.timedelta(days=1)
            else:
                base_date = max(
                    date for date in (base_watermark, max_order_date) if date is not None
                )

            cursor.execute("SELECT MAX(orderNumber) AS next_number FROM orders")
            row = cursor.fetchone()
            next_order_number = int(row["next_number"] or 0) + 1

            created_orders = []
            created_details = 0
            first_date = None
            last_date = None

            for index in range(args.count):
                order_number = next_order_number + index
                order_date = base_date + datetime.timedelta(days=index + 1)
                required_date = order_date + datetime.timedelta(days=7)
                shipped_date = order_date + datetime.timedelta(days=1)
                customer_number, product_code, buy_price = choose_random_existing_items(cursor)
                quantity = random.randint(1, 10)
                price_each = round(buy_price, 2)
                order_line_number = 1

                cursor.execute(
                    "INSERT INTO orders "
                    "(orderNumber, orderDate, requiredDate, shippedDate, status, comments, customerNumber) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        order_number,
                        order_date,
                        required_date,
                        shipped_date,
                        "Shipped",
                        None,
                        customer_number,
                    ),
                )

                cursor.execute(
                    "INSERT INTO orderdetails "
                    "(orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        order_number,
                        product_code,
                        quantity,
                        price_each,
                        order_line_number,
                    ),
                )

                created_orders.append(order_number)
                created_details += 1
                first_date = first_date or order_date
                last_date = order_date

            conn.commit()

    print("Simulation completed")
    print(f"Created orders: {created_orders}")
    print(f"Dates: {first_date} -> {last_date}")
    print(f"Order detail rows inserted: {created_details}")


def validate_incremental_source(args):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (WATERMARK_TABLE,),
            )
            if cursor.fetchone()["cnt"] == 0:
                print(f"Missing table: {WATERMARK_TABLE}")
                raise SystemExit(1)

            watermark = get_watermark(cursor)
            if watermark is None:
                print(f"Missing watermark row for pipeline {PIPELINE_NAME}")
                raise SystemExit(1)
            if watermark["last_processed_order_date"] is None:
                print("Watermark row exists, but last_processed_order_date is NULL")
                raise SystemExit(1)

            cursor.execute(
                "SELECT MAX(orderDate) AS max_date FROM orders"
            )
            max_order_date = cursor.fetchone()["max_date"]
            if max_order_date is None:
                print("orders table is empty or unavailable")
                raise SystemExit(1)

            pending_query = (
                "SELECT o.orderNumber, o.orderDate, COUNT(d.orderNumber) AS detail_count "
                "FROM orders o "
                "LEFT JOIN orderdetails d ON o.orderNumber = d.orderNumber "
                "WHERE o.orderDate > %s "
                "GROUP BY o.orderNumber, o.orderDate"
            )
            cursor.execute(pending_query, (watermark["last_processed_order_date"],))
            pending_orders = cursor.fetchall()

            print(f"Watermark last_processed_order_date: {watermark['last_processed_order_date']}")
            print(f"Max orders.orderDate: {max_order_date}")
            print(f"Pending orders after watermark: {len(pending_orders)}")

            if pending_orders:
                bad_orders = [row for row in pending_orders if row["detail_count"] == 0]
                if bad_orders:
                    print("Found orders after watermark without orderdetails:")
                    for bad in bad_orders:
                        print(f" orderNumber={bad['orderNumber']} orderDate={bad['orderDate']}")
                    raise SystemExit(1)

            print("Incremental source validation passed.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Assignment 2 Task 1 incremental source helper."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init_watermark", help="Create watermark metadata baseline")
    init_parser.set_defaults(func=init_watermark)

    sim_parser = subparsers.add_parser("simulate_new_orders", help="Insert new orders after the current watermark")
    sim_parser.add_argument("--count", type=int, default=5, help="Number of new orders to create")
    sim_parser.add_argument("--seed", type=int, default=None, help="Optional seed for reproducible data")
    sim_parser.set_defaults(func=simulate_new_orders)

    validate_parser = subparsers.add_parser(
        "validate_incremental_source",
        help="Validate watermark and pending incremental orders",
    )
    validate_parser.set_defaults(func=validate_incremental_source)

    return parser.parse_args()


def main():
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
