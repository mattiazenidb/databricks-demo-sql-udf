-- Databricks notebook source
-- MAGIC %run ./setup

-- COMMAND ----------

CREATE TABLE database_mattia_demos_sql_udf.dataset_2 AS SELECT * FROM database_mattia_demos_sql_udf.dataset

-- COMMAND ----------

SELECT * FROM database_mattia_demos_sql_udf.dataset

-- COMMAND ----------

SELECT * FROM database_mattia_demos_sql_udf.dataset_2

-- COMMAND ----------

CREATE FUNCTION database_mattia_demos_sql_udf.test_udf(x INT)
  RETURNS STRING
  COMMENT 'Multiplies the input by 2'
  RETURN x * 2

-- COMMAND ----------

SELECT *, database_mattia_demos_sql_udf.test_udf(AGE) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

SELECT *
    FROM database_mattia_demos_sql_udf.dataset
    RIGHT JOIN database_mattia_demos_sql_udf.dataset_2 ON database_mattia_demos_sql_udf.test_udf(database_mattia_demos_sql_udf.dataset.AGE) = database_mattia_demos_sql_udf.dataset_2.ID;

-- COMMAND ----------

DELETE FROM database_mattia_demos_sql_udf.dataset WHERE database_mattia_demos_sql_udf.mzeni_test_sql_udf(AGE) == LABEL
