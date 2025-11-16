using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Aggregated statistics about a single ETL run, returned by the ETL pipeline.
    /// </summary>
    public sealed class TripImportStatsDto
    {
        /// <summary>
        /// Total number of CSV data rows read (excluding header).
        /// </summary>
        public int TotalRowsRead { get; init; }

        /// <summary>
        /// Number of rows that passed basic parsing/validation and were converted
        /// into domain <c>Trip</c> instances before duplicate detection.
        /// </summary>
        public int ParsedRows { get; init; }

        /// <summary>
        /// Number of rows that were rejected due to parsing/validation errors
        /// (e.g. invalid date, negative distance, invalid flag value, etc.).
        /// </summary>
        public int InvalidRows { get; init; }

        /// <summary>
        /// Number of rows identified as duplicates according to the deduplication key
        /// (pickup, dropoff, passenger_count) and therefore not sent to the database.
        /// </summary>
        public int DuplicateRows { get; init; }

        /// <summary>
        /// Number of rows that were actually sent to the database via bulk insert.
        /// </summary>
        public int InsertedRows { get; init; }

        /// <summary>
        /// Number of duplicate rows written to the duplicates.csv file.
        /// Often equal to <see cref="DuplicateRows"/>, but kept separate
        /// to allow future changes in file-writing logic.
        /// </summary>
        public int DuplicatesFileRows { get; init; }
    }
}
