# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── output/task1/part-00000-3be55839-2260-44c5-b95c-a20319e2faf1-c000.csv
│   |    └── output/task1/part-00000-c4a9eb8b-a202-44e4-9641-d13f1cf87d95-c000.csv
│   |    └── output/task1/part-00000-d50ef07b-76e2-4a2d-92c3-561a00a024ee-c000.csv
|   ├── task_2
│   |    └── output/task2/epoch=12/part-00000-bf121cea-773f-434b-bc03-403a95c44ec5-c000.csv
│   |    └── output/task2/epoch=5/part-00000-ea990815-a8f3-4305-a358-469fa08019b5-c000.csv
│   |    └── output/task2/epoch=6/part-00000-b0983dd1-2f91-4101-9afd-3c24944ec64c-c000.csv
|   └── task_3
│       └── output/task3/epoch=3/part-00000-578253c0-5ead-49ea-827b-b2c4856a2d01-c000.csv
│       └── output/task3/epoch=5/part-00000-7ef6c197-69ec-4c3e-87ba-9be937fb5531-c000.csv
│       └── output/task3/epoch=9/part-00000-df399804-72eb-4474-8343-3ee7feb93e2c-c000.csv
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Instructions:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Print the parsed data to the console (using .writeStream.format("console")).

## **Sample output:**
  ```bash
    
trip_id,driver_id,distance_km,fare_amount,timestamp
b17e7889-2625-4938-8fe8-7afb9be8bf16,98,15.89,144.62,2025-10-16 05:06:37
  ```
---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Instructions:**
1. Reuse the parsed DataFrame from Task 1.
2. Group by driver_id and compute:
3. SUM(fare_amount) as total_fare
4. AVG(distance_km) as avg_distance
5. Store the result in csv

## **Sample output:**
```bash
driver_id,total_fare,avg_distance
12,125.91,15.99
1,207.26,20.44
13,126.43,12.92
16,96.22,39.81
5,58.42,35.93
29,158.48,41.21
68,118.95,15.65
67,91.95,36.965

 ```
---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

```bash
window_start,window_end,total_fare
2025-10-16T05:07:00.000Z,2025-10-16T05:12:00.000Z,1905.96
2025-10-16T05:09:00.000Z,2025-10-16T05:14:00.000Z,1905.96
2025-10-16T05:06:00.000Z,2025-10-16T05:11:00.000Z,1905.96
2025-10-16T05:10:00.000Z,2025-10-16T05:15:00.000Z,1905.96
2025-10-16T05:08:00.000Z,2025-10-16T05:13:00.000Z,1905.96
    
 ```

---

