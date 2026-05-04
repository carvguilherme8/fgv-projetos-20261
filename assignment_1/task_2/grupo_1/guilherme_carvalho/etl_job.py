import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_OUTPUT_PATH",
    "JDBC_CONNECTION_URL",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
    "CONNECTION_NAME"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_output = args["S3_OUTPUT_PATH"]
jdbc_url = args["JDBC_CONNECTION_URL"]
db_user = args["DB_USER"]
db_password = args["DB_PASSWORD"]
db_name = args["DB_NAME"]

connection_options = {
    "url": jdbc_url,
    "user": db_user,
    "password": db_password,
}


def read_table(table_name):
    return spark.read.format("jdbc").options(
        **connection_options,
        dbtable=f"{db_name}.{table_name}"
    ).load()


# Extract

customers_df = read_table("customers")
products_df = read_table("products")
orders_df = read_table("orders")
orderdetails_df = read_table("orderdetails")
offices_df = read_table("offices")
employees_df = read_table("employees")

# Transformation

# dim_customers
dim_customers = customers_df.select(
    F.col("customerNumber").alias("customer_id"),
    F.col("customerName").alias("customer_name"),
    F.concat_ws(" ", F.col("contactFirstName"), F.col("contactLastName")).alias("contact_name"),
    F.col("city"),
    F.col("country")
)

# dim_products
dim_products = products_df.select(
    F.col("productCode").alias("product_id"),
    F.col("productName").alias("product_name"),
    F.col("productLine").alias("product_line"),
    F.col("productVendor").alias("product_vendor")
)

# dim_dates (derived from distinct order dates)
dim_dates = orders_df.select(
    F.col("orderDate")
).distinct().select(
    F.date_format("orderDate", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    F.col("orderDate").alias("full_date"),
    F.year("orderDate").alias("year"),
    F.quarter("orderDate").alias("quarter"),
    F.month("orderDate").alias("month"),
    F.dayofmonth("orderDate").alias("day")
)

# dim_countries (customer countries mapped to territories via sales reps)
customer_territory = (
    customers_df
    .join(employees_df, customers_df.salesRepEmployeeNumber == employees_df.employeeNumber, "left")
    .join(offices_df, employees_df.officeCode == offices_df.officeCode, "left")
    .select(
        customers_df.country,
        offices_df.territory
    )
    .distinct()
)

office_territories = offices_df.select("country", "territory").distinct()
all_territories = customer_territory.unionByName(office_territories).distinct()

dim_countries = all_territories.select(
    F.md5(F.col("country")).alias("country_key"),
    F.col("country"),
    F.coalesce(F.col("territory"), F.lit("N/A")).alias("territory")
).dropDuplicates(["country"])

# fact_orders
fact_orders = (
    orderdetails_df
    .join(orders_df, "orderNumber")
    .join(customers_df, orders_df.customerNumber == customers_df.customerNumber, "left")
    .select(
        F.col("orderNumber").alias("order_id"),
        orders_df.customerNumber.alias("customer_id"),
        F.col("productCode").alias("product_id"),
        F.date_format("orderDate", "yyyyMMdd").cast(IntegerType()).alias("order_date_key"),
        F.md5(customers_df.country).alias("country_key"),
        F.col("quantityOrdered").alias("quantity_ordered"),
        F.col("priceEach").alias("price_each"),
        (F.col("quantityOrdered") * F.col("priceEach")).alias("sales_amount")
    )
)

# Load

fact_orders.write.mode("overwrite").parquet(f"{s3_output}/fact_orders")
dim_customers.write.mode("overwrite").parquet(f"{s3_output}/dim_customers")
dim_products.write.mode("overwrite").parquet(f"{s3_output}/dim_products")
dim_dates.write.mode("overwrite").parquet(f"{s3_output}/dim_dates")
dim_countries.write.mode("overwrite").parquet(f"{s3_output}/dim_countries")

job.commit()
