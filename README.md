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





How to: 

- clone repo
- download data from kaggle
  - unzip .zip file
  - copy archive folder into data/ 
    
- run: $ mill spark.standalone.run


