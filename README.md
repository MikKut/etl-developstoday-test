# C# Test Assessment

The goal of this task is to implement a simple ETL project in CLI that inserts data from a CSV into a single, flat table. While the task is straightforward, I took care to read and follow the specification closely.

Input CSV data file: `data/sample-cab-data.csv`.

## Objectives

1. Import the data from the CSV into an MS SQL table. Only the following columns are stored:
    - `tpep_pickup_datetime`
    - `tpep_dropoff_datetime`
    - `passenger_count`
    - `trip_distance`
    - `store_and_fwd_flag`
    - `PULocationID`
    - `DOLocationID`
    - `fare_amount`
    - `tip_amount`
2. Set up a SQL Server database (local or cloud-based, as convenient).
3. Design a table schema that will hold the processed data, using appropriate data types.
4. Optimize the schema for the following queries:
    - Find out which `PULocationId` (Pick-up location ID) has the highest average `tip_amount`.
    - Find the top 100 longest fares in terms of `trip_distance`.
    - Find the top 100 longest fares in terms of time spent traveling.
    - Search where part of the conditions is `PULocationId`.
5. Implement efficient bulk insertion of the processed records into the database.
6. Identify and remove any duplicate records from the dataset based on a combination of `pickup_datetime`, `dropoff_datetime`, and `passenger_count`. Write all removed duplicates into a `duplicates.csv` file.
7. For the `store_and_fwd_flag` column, convert any `'N'` values to `'No'` and any `'Y'` values to `'Yes'`.
8. Ensure that all text-based fields are free from leading or trailing whitespace.
9. Assume the program will be used on much larger data files. Describe in a few sentences what would change if it were used for a 10GB CSV input file.
10. *(Nice to have)* The input data is in the EST timezone. Convert it to UTC when inserting into the DB.

## Requirements

- Use C# as the primary programming language.
- Ensure efficient data insertion into SQL Server.
- Assume the data comes from a potentially unsafe source.

## Deliverables

- Project source code.
- SQL scripts used for creating the database and tables.
- Number of rows in the table after running the program:  
  **Total=30000, Parsed=29855, Invalid=145, Duplicates=15, Inserted=29840, DuplicatesFile=15**
- Any comments on any assumptions made (see below).

## Assumptions & Comments

Below are the main assumptions made while implementing the solution, together with short comments on why they were chosen.

1. **Input CSV format**
   - **Assumption:** The input file has a header row with standard NYC yellow taxi field names (e.g. `tpep_pickup_datetime`, `passenger_count`, `fare_amount`, etc.) and is comma-separated.
   - **Comment:** The ETL uses these header names (via `TripFieldNames`) to map columns and will fail fast if required columns are missing.

2. **Time zone**
   - **Assumption:** All pickup and dropoff timestamps in the input are in **EST**, without per-row time zone information.
   - **Comment:** Timestamps are first parsed as local (unspecified) and then converted from EST to UTC before being stored in the database. This keeps the DB consistently in UTC, which is usually preferred for analytics and avoids ambiguity.

3. **Validation vs. data loss**
   - **Assumption:** Only **format-level** errors (missing values, non-parsable dates/numbers) cause a row to be discarded; suspicious but parsable values (e.g. negative amounts or distances) are imported as-is.
   - **Comment:** This avoids silently dropping rows and helps keep the row count aligned with the original dataset, as required by the task. Stricter business validation rules (e.g. `fare_amount >= 0`) can be added in a production setting if the domain requires it.

4. **Definition of a duplicate**
   - **Assumption:** Duplicates are defined exactly as records that share the same combination of  
     `(pickup_datetime, dropoff_datetime, passenger_count)` **after** converting timestamps to UTC.
   - **Comment:** Conversion from EST to UTC shifts both timestamps by the same offset, so relative equality is preserved. Duplicates are removed before insertion into the `Trips` table, and all removed records are written to `duplicates.csv`.

5. **`store_and_fwd_flag` semantics**
   - **Assumption:** Only the values `'N'` and `'Y'` are considered meaningful in the input for `store_and_fwd_flag`.
   - **Comment:** These values are trimmed, upper-cased and mapped to a small enum (`No` / `Yes`), then stored in the database as `"No"` / `"Yes"`. Any other value is treated as invalid for this task and the row is skipped.

6. **Identity key**
   - **Assumption:** The CSV does not provide a stable primary key, so a surrogate integer key is used in the database (`Id BIGINT IDENTITY`).
   - **Comment:** This keeps the physical schema simple and does not affect the analytical queries, which are based on the domain columns rather than on `Id`.

7. **In-memory duplicate detection**
   - **Assumption:** For the provided sample file and typical small/medium datasets, an in-memory `HashSet<(pickup, dropoff, passenger_count)>` is sufficient for duplicate detection.
   - **Comment:** For truly large files (e.g. 10GB), this approach may put pressure on memory. In that scenario, I would move deduplication closer to the database (using a staging table plus `ROW_NUMBER()/PARTITION BY` to keep the first occurrence and mark the rest as duplicates) or combine per-batch in-memory deduplication with a `UNIQUE` index in SQL Server to enforce cross-batch uniqueness.

## Large File Scenario (10GB)

If the input file were around 10GB, I would adjust the design as follows:

- **Deduplication:**  
  Move the main deduplication logic to the database by loading data into a staging table and using a SQL query with `ROW_NUMBER() OVER (PARTITION BY pickup_datetime, dropoff_datetime, passenger_count)` to:
  - insert only the first occurrence (`rn = 1`) into the final `Trips` table;
  - treat rows with `rn > 1` as duplicates and export them to `duplicates.csv`.

- **Memory usage:**  
  Keep the current batching strategy (processing the CSV in chunks and bulk-inserting each batch) but tune the batch size to balance throughput and memory usage. If necessary, replace the `DataTable`-based bulk insert with a streaming `DbDataReader`-based approach.

- **Scalability:**  
  Optionally introduce configuration flags to switch between the simple in-memory deduplication (for smaller files) and the database-backed deduplication strategy (for very large inputs).