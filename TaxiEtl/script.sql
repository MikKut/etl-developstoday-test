--------------------------------------------------------------------
-- create_trips_table.sql
-- SQL script used to create the TaxiEtl database and Trips table
-- used by the ETL CLI.
--------------------------------------------------------------------

--------------------------------------------------------------------
-- 1. (Optional) Create database
--------------------------------------------------------------------
IF DB_ID(N'TaxiEtl') IS NULL
BEGIN
    CREATE DATABASE [TaxiEtl];
END
GO

USE [TaxiEtl];
GO

--------------------------------------------------------------------
-- 2. Drop existing Trips table (for repeatable runs)
--------------------------------------------------------------------
IF OBJECT_ID(N'dbo.Trips', N'U') IS NOT NULL
BEGIN
    DROP TABLE [dbo].[Trips];
END
GO

--------------------------------------------------------------------
-- 3. Create Trips table
--------------------------------------------------------------------
CREATE TABLE [dbo].[Trips]
(
    [Id]                    BIGINT IDENTITY(1,1) NOT NULL,
    [tpep_pickup_datetime]  DATETIME2(0)         NOT NULL,
    [tpep_dropoff_datetime] DATETIME2(0)         NOT NULL,
    [passenger_count]       TINYINT              NOT NULL,
    [trip_distance]         DECIMAL(9,3)         NOT NULL,
    [store_and_fwd_flag]    VARCHAR(3)           NOT NULL,
    [PULocationID]          INT                  NOT NULL,
    [DOLocationID]          INT                  NOT NULL,
    [fare_amount]           DECIMAL(10,2)        NOT NULL,
    [tip_amount]            DECIMAL(10,2)        NOT NULL,

    [TravelTimeSeconds] AS DATEDIFF(SECOND, [tpep_pickup_datetime], [tpep_dropoff_datetime]) PERSISTED,

    CONSTRAINT [PK_Trips] PRIMARY KEY CLUSTERED ([Id] ASC)
);
 
GO

--------------------------------------------------------------------
-- 4. Indexes for required analytical queries
--------------------------------------------------------------------

-- 4.1. For:
-- "Which PULocationID has the highest tip_amount on average?"
-- Index on PULocationID with included tip_amount.
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Trips_PULocation_TipAmount'
      AND object_id = OBJECT_ID(N'dbo.Trips')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Trips_PULocation_TipAmount]
    ON [dbo].[Trips] ([PULocationID])
    INCLUDE ([tip_amount]);
END
GO

-- 4.2. For:
-- "Top 100 longest fares in terms of trip_distance"
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Trips_TripDistance'
      AND object_id = OBJECT_ID(N'dbo.Trips')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Trips_TripDistance]
    ON [dbo].[Trips] ([trip_distance]);
END
GO

-- 4.3. For:
-- "Top 100 longest fares in terms of time spent traveling"
-- Uses the computed column TravelTimeSeconds.
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Trips_TravelTimeSeconds'
      AND object_id = OBJECT_ID(N'dbo.Trips')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Trips_TravelTimeSeconds]
    ON [dbo].[Trips] ([TravelTimeSeconds]);
END
GO