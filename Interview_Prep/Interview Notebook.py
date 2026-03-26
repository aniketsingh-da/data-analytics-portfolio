# Databricks notebook source
# MAGIC %md
# MAGIC ####Data Analyst Interview Preparation – SQL & Python Exercises
# MAGIC
# MAGIC #####Description:
# MAGIC - This workbook provides junior data analysts with practical SQL and Python exercises commonly asked in interviews. It includes examples on salary analysis, window functions, palindrome and factorial programs, punch-in/out tracking, and calculating total hours worked. Each task demonstrates real-world scenarios and problem-solving techniques to enhance interview readiness.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Task 1: 
# MAGIC
# MAGIC Find the second highest salary from the Employee table for each Department ( 2 Query)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Employee (
# MAGIC   S_NO INT,
# MAGIC   NAME STRING,
# MAGIC   DEPARTMENT_ID INT,
# MAGIC   SALARY INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Employee VALUES
# MAGIC (1, 'Alice', 101, 120000),
# MAGIC (2, 'Bob', 101, 80000),
# MAGIC (3, 'Charlie', 102, 150000),
# MAGIC (4, 'David', 102, 50000),
# MAGIC (5, 'Eve', 103, 130000),
# MAGIC (6, 'Frank', 103, 70000),
# MAGIC (7, 'Grace', 104, 110000),
# MAGIC (8, 'Henry', 104, 90000),
# MAGIC (9, 'Irene', 105, 190000),
# MAGIC (10, 'Jack', 105, 10000);
# MAGIC
# MAGIC SELECT * FROM Employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1st way
# MAGIC
# MAGIC WITH SECOND_HIGHEST_SALARY AS (
# MAGIC     SELECT NAME,
# MAGIC           SALARY,
# MAGIC           DEPARTMENT_ID,
# MAGIC           DENSE_RANK() OVER (PARTITION BY DEPARTMENT_ID ORDER BY SALARY DESC) AS RANK
# MAGIC     FROM Employee)
# MAGIC   
# MAGIC SELECT DEPARTMENT_ID,
# MAGIC        NAME,
# MAGIC        SALARY
# MAGIC   FROM second_highest_salary
# MAGIC   WHERE RANK = 2
# MAGIC   ORDER  BY 3 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2nd Way
# MAGIC SELECT 
# MAGIC     E.DEPARTMENT_ID,
# MAGIC     E.NAME,
# MAGIC     E.SALARY
# MAGIC FROM EMPLOYEE E
# MAGIC WHERE E.SALARY = (
# MAGIC     SELECT MAX(F.SALARY)
# MAGIC     FROM EMPLOYEE F
# MAGIC     WHERE F.DEPARTMENT_ID = E.DEPARTMENT_ID
# MAGIC       AND F.SALARY < (
# MAGIC           SELECT MAX(SALARY)
# MAGIC           FROM EMPLOYEE
# MAGIC           WHERE DEPARTMENT_ID = F.DEPARTMENT_ID
# MAGIC       )
# MAGIC )
# MAGIC   ORDER BY 3 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Task 2:
# MAGIC
# MAGIC Write an SQL query to generate a department-wise salary report with the following requirements:
# MAGIC
# MAGIC 1. Calculate the total salary paid in each department.
# MAGIC 2. Identify the employee(s) who have the highest salary in each department.
# MAGIC 3. For each employee, compute their salary as a percentage of the total salary of their respective department.
# MAGIC 4. Create a column optimization_potential:
# MAGIC Return 'YES' if the employee’s salary contribution is greater than 50% of the department total
# MAGIC Otherwise return 'NO'
# MAGIC 5. Create a column highest_salary_flag:
# MAGIC Return 'HIGHEST' for employees with the maximum salary in their department
# MAGIC Otherwise return 'NORMAL'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH BASE_TABLE AS (
# MAGIC     SELECT 
# MAGIC         S_NO, 
# MAGIC         NAME,
# MAGIC         DEPARTMENT_ID,
# MAGIC         SALARY,
# MAGIC         SUM(SALARY) OVER (PARTITION BY DEPARTMENT_ID) AS TOTAL_SALARY_PER_DEPARTMENT
# MAGIC     FROM EMPLOYEE
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     S_NO,
# MAGIC     NAME, 
# MAGIC     DEPARTMENT_ID,
# MAGIC     SALARY, 
# MAGIC     TOTAL_SALARY_PER_DEPARTMENT,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         ROUND(SALARY * 100.0 / TOTAL_SALARY_PER_DEPARTMENT, 1),
# MAGIC         '%'
# MAGIC     ) AS SALARY_PERCENTAGE,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN SALARY * 100.0 / TOTAL_SALARY_PER_DEPARTMENT < 50 THEN 'NO'
# MAGIC         ELSE 'YES'
# MAGIC     END AS OPTIMIZATION_POTENTIAL,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN SALARY = MAX(SALARY) OVER (PARTITION BY DEPARTMENT_ID) 
# MAGIC         THEN 'HIGHEST'
# MAGIC         ELSE 'NORMAL'
# MAGIC     END AS HIGHEST_SALARY_PER_DEPARTMENT
# MAGIC
# MAGIC FROM BASE_TABLE;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Python
# MAGIC
# MAGIC ######Task 3:
# MAGIC
# MAGIC Write a Python function that takes a string as input and checks whether it is a palindrome or not.
# MAGIC
# MAGIC - A palindrome is a word that reads the same forward and backward (e.g., "madam", "racecar").
# MAGIC Return True if the input is a palindrome, otherwise return False.

# COMMAND ----------


def palindrome2(name):
    s = str(name)
    return s == s[::-1]

name = input("Enter a Name: ")
print(palindrome2(name))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Task 4:
# MAGIC
# MAGIC Write a Python function to calculate the factorial of a given number using recursion.
# MAGIC
# MAGIC - Factorial of a number n is the product of all positive integers less than or equal to n.
# MAGIC
# MAGIC - Example: 5! = 5 × 4 × 3 × 2 × 1 = 120
# MAGIC The function should take an integer input and return its factorial.

# COMMAND ----------

def fact(n):
    if n==0 or n==1:
        return 1
    else:
        return n * fact(n-1)

n = int(input("Enter a number: "))
print("Factorial:", fact(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Task 5:
# MAGIC
# MAGIC Write an SQL query to identify employees whose most recent punch record indicates they are currently inside the office.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE Punch_Log (
# MAGIC     ID INT,
# MAGIC     EMPLOYEE_ID INT,
# MAGIC     PUNCH_TIME TIMESTAMP,
# MAGIC     STATUS STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Punch_Log VALUES
# MAGIC (1, 101, '2025-06-25 08:00:00', 'IN'),
# MAGIC (2, 102, '2025-06-25 08:15:00', 'IN'),
# MAGIC (3, 101, '2025-06-25 17:00:00', 'OUT'),
# MAGIC (4, 103, '2025-06-25 09:00:00', 'IN'),
# MAGIC (5, 102, '2025-06-25 12:00:00', 'OUT'),
# MAGIC (6, 104, '2025-06-25 09:30:00', 'IN'),
# MAGIC (7, 105, '2025-06-25 10:00:00', 'IN'),
# MAGIC (8, 105, '2025-06-25 16:30:00', 'OUT'),
# MAGIC (9, 104, '2025-06-25 18:00:00', 'OUT'),
# MAGIC (10, 103, '2025-06-25 18:15:00', 'OUT'),
# MAGIC (11, 103, '2025-06-26 08:31:00', 'IN');
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM Punch_Log
# MAGIC ORDER BY 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH EMP_STATUS AS (
# MAGIC     SELECT ID,
# MAGIC        EMPLOYEE_ID,
# MAGIC        PUNCH_TIME,
# MAGIC        STATUS, 
# MAGIC        ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID ORDER BY PUNCH_TIME DESC) AS NEXT_ENTRY
# MAGIC     FROM PUNCH_LOG )
# MAGIC
# MAGIC SELECT EMPLOYEE_ID,
# MAGIC        PUNCH_TIME,
# MAGIC        STATUS
# MAGIC FROM EMP_STATUS
# MAGIC WHERE NEXT_ENTRY = 1
# MAGIC AND STATUS = 'IN'; 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Task 6:
# MAGIC
# MAGIC Write an SQL query to calculate the total number of hours each employee spends inside the office

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH TIME AS (
# MAGIC SELECT EMPLOYEE_ID,
# MAGIC       STATUS,
# MAGIC       LEAD(STATUS) OVER (PARTITION BY EMPLOYEE_ID ORDER BY PUNCH_TIME) AS NEXT_STATUS,
# MAGIC       PUNCH_TIME,
# MAGIC       LEAD(PUNCH_TIME) OVER (PARTITION BY EMPLOYEE_ID ORDER BY PUNCH_TIME) AS NEXT_PUNCH_TIME
# MAGIC FROM PUNCH_LOG
# MAGIC ORDER BY EMPLOYEE_ID, PUNCH_TIME)
# MAGIC
# MAGIC SELECT EMPLOYEE_ID,
# MAGIC        DATEDIFF(HOUR, PUNCH_TIME, NEXT_PUNCH_TIME) AS TIME_SPEND_IN_OFFICE
# MAGIC FROM TIME
# MAGIC WHERE STATUS = 'IN'
# MAGIC AND NEXT_STATUS = 'OUT';

# COMMAND ----------

# MAGIC %md
# MAGIC