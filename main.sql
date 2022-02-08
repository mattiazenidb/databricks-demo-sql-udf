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

-- MAGIC %md
-- MAGIC 
-- MAGIC Ad esempio usandola in una **DELETE** ci ritorna: Using SQL function 'dbo.start_month' in DeltaDelete is not supported. Abbiamo errori simili anche nel caso di JOIN.

-- COMMAND ----------

DELETE FROM database_mattia_demos_sql_udf.dataset WHERE database_mattia_demos_sql_udf.test_udf(AGE) == LABEL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Ad esempio usandola in una DELETE ci ritorna: Using SQL function 'dbo.start_month' in DeltaDelete is not supported. Abbiamo errori simili anche nel caso di **JOIN**.

-- COMMAND ----------

SELECT id
    FROM database_mattia_demos_sql_udf.dataset
    RIGHT JOIN database_mattia_demos_sql_udf.dataset_2 ON database_mattia_demos_sql_udf.test_udf(database_mattia_demos_sql_udf.dataset.AGE) = database_mattia_demos_sql_udf.dataset_2.LABEL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Usando il metodo spiegato in documentazione ci ritorna un altro errore quando vogliamo creare viste non temporanee: 'F_STARTYEAR'. This function is neither a registered temporary function nor a permanent function registered in the database 'default' Abbiamo provato ad esplicitare le UDF quando trovavamo questi errori, ma non sempre è fattibile.

-- COMMAND ----------

-- Create or replace view for `experienced_employee` with comments.
CREATE OR REPLACE VIEW database_mattia_demos_sql_udf.test_view_sql
    AS SELECT *, database_mattia_demos_sql_udf.mzeni_test_sql_udf(AGE) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

SELECT * FROM database_mattia_demos_sql_udf.test_view_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def mzeni_test_python_udf(age):
-- MAGIC   return 'OLD'
-- MAGIC spark.udf.register("mzeni_test_python_udf", mzeni_test_python_udf)

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED database_mattia_demos_sql_udf.mzeni_test_sql_udf

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED mzeni_test_python_udf

-- COMMAND ----------

SELECT *, mzeni_test_python_udf(AGE) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

-- Create or replace view for `experienced_employee` with comments.
CREATE OR REPLACE TEMPORARY VIEW test_view_python
    AS SELECT *, mzeni_test_python_udf(AGE) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

SELECT * FROM test_view_python

-- COMMAND ----------

-- Create or replace view for `experienced_employee` with comments.
CREATE OR REPLACE VIEW database_mattia_demos_sql_udf.test_view_python
    AS SELECT *, mzeni_test_python_udf(AGE) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

SELECT * FROM database_mattia_demos_sql_udf.test_view_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Test permanent view using a function from a .jar in scala

-- COMMAND ----------

CREATE OR REPLACE FUNCTION database_mattia_demos_sql_udf.ValidateIBAN AS 'com.ing.wbaa.spark.udf.ValidateIBAN'
    USING JAR '/FileStore/spark_udf_assembly_0.2.0.jar';

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED database_mattia_demos_sql_udf.ValidateIBAN

-- COMMAND ----------

SELECT *, database_mattia_demos_sql_udf.ValidateIBAN(NAME) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;

-- COMMAND ----------

CREATE OR REPLACE VIEW database_mattia_demos_sql_udf.test_view_python_2
    AS SELECT *, database_mattia_demos_sql_udf.ValidateIBAN(NAME) AS LABEL2 FROM database_mattia_demos_sql_udf.dataset;
