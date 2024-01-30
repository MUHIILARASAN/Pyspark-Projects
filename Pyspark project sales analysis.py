# Databricks notebook source
## /FileStore/tables/menu_csv.txt

## /FileStore/tables/sales_csv-1.txt



## Sales dataframe
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType


schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True),
])


sales_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv-1.txt")
display(sales_df)


## Deriving year, month, quarter

from pyspark.sql.functions import month, year, quarter

sales_df = sales_df.withColumn("order_year",year(sales_df.order_date))
display(sales_df)



## Like Month here we will be extracting querter , month
sales_df = sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
sales_df = sales_df.withColumn("order_month",month(sales_df.order_date))
display(sales_df)



## Menu Dataframe
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DataType
schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True),
])

menu_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")
display(menu_df)



## Total amount spent by each customer
total_amount_spent = (sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({"price":"sum"})
                      .orderBy('customer_id'))
display(total_amount_spent)



## Total amount spent by each food category
amount_spent_for_each_food = (sales_df.join(menu_df,"product_id").groupBy("product_name").agg({"price":"sum"})
                              .orderBy("product_name"))
display(amount_spent_for_each_food)



## Total amount of sales in each month
total_amount_of_sales = (sales_df.join(menu_df,"product_id").groupBy("order_month").agg({'price':'sum'})
                         .orderBy('order_month'))
display(total_amount_of_sales)



##Yearly Sales
yearly_sales = (sales_df.join(menu_df,"product_id").groupBy('order_year').agg({'price':'sum'})
                .orderBy('order_year'))
display(yearly_sales)



##Quarterly Sales
quarterly_sales = (sales_df.join(menu_df,'product_id').groupBy('order_quarter').agg({'price':'sum'})
                   .orderBy('order_quarter'))
display(quarterly_sales)



##How many times each product purchased
from pyspark.sql.functions import count

most_df = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name')
           .agg(count('product_id').alias('product_count')).
           orderBy('product_count', ascending = 0)
           .drop('product_id')
           )
display(most_df)




top_order_item = (sales_df.join(menu_df,"product_id").groupBy("product_id","product_name")
                  .agg(count('product_id').alias('product_count')).
                  orderBy('product_count', ascending=0)
                  .drop('product_id').limit(1)
                  )
display(top_order_item)



##Frequency of customer visited to restarants 
from pyspark.sql.functions import countDistinct

df = (sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date'))
)
display(df)
      



##Total sales by each country
total_sales_by_each_country = (sales_df.join(menu_df,'product_id').groupBy('Location').agg({'price':'sum'}))
display(total_sales_by_each_country)



#Total sales by order source
total_order_source = (sales_df.join(menu_df).groupBy('source_order').agg({'price':'sum'}))
display(total_order_source)




