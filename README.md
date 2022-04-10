# Olist Assignment - Data Pipeline 2

Requirements:
- Using olist dataset https://www.kaggle.com/olistbr/brazilian-ecommerce, identify all late deliveries, 
  so that we can provide a 10% discount on next delivery to boost the sales.
- **order_purchase_timestamp** is by default in Sao Paulo timezone.
- **order_delivered_customer_date** is by default in the customer delivery address timezone.


You must provide the result:

- A csv file with list of customers having late deliveries (more than 10 days)


You must explain how to reproduce the result:

- git clone <repository>

- a README.md file explain how to reproduce the result



Evaluation (100 points, 50 points required to have the module)

- git clone <repository> with a README.md: 10 points

- the README.md explain how to run the batch: 5 points

- following README.md, the batch is running without error: 5 points

- the batch generates a single CSV output file: 5 points

- the CSV output file contains a list of customer identifiers: 5 points

- all customer identifiers have a late delivery: 10 points

- all customers having a late delivery are exported: 10 points

- a procedure to run test is provided: 5 points

- a test coverage report is provided: 5 points

- all tests are meaningful and test coverage > 80% of line of code : 15 points

- the README.md explain how to package & run the batch on Amazon: 15 points

- an architecture document explaining the solution is provided, including diagram(s) and explanations:10 points



### Pre-requisites: 
- Install Apache Spark
- Install mill `brew install mill`

### How to run: 

- Clone repo `git clone https://github.com/dianapatrong/datapipeline2-olist-assignment.git`
- Download data from kaggle https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  - Unzip the `archive.zip` file
  - Copy archive folder into the folder `data/` inside the repository
    
- Run: `mill spark.standalone.run`
  ![Result](documentation/ResultsOnTerminal.png)
  

- Go to `output/late_deliveries` folder to get the result in CSV format


### Result explanation
 For getting the deliveries that were late for more than 10 days I used the following files: 
 * `olist_orders_dataset.csv` -> to get information regarding which orders were late and from which customers 
 * `olist_order_items_dataset.csv` -> helper to identify which kind of product was ordered
 * `olist_products_dataset.csv` -> to identify the kind of product that was being delivered late

1. Read the `olist_orders_dataset.csv` and convert `order_delivered_customer_date` and `order_purchase_timestamp` into UTC timestamp from a timezone of Sao Paolo
```
NOTE: I'm taking into account that the columns order_purchase_timestamp and order_delivered_customer_date are by default in Sao Paolo timezone 
```
2. Calculate the difference in days from the columns previously converted to get the late deliveries
3. Filter the data to only include the deliveries that were more than 10 days late

Extra:
Since I wanted to know what kind of products were delivered late I did the following: 
4. Read the `olist_order_items_dataset.csv` and `olist_products_dataset.csv` csv's and joined them to get the product information.

![Olist sources](documentation/olist_sources.png)

