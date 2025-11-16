using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Configuration settings for the ETL process. Typically bound from configuration
    /// (e.g. "Etl" section in appsettings.json) and injected as IOptions&lt;EtlSettings&gt;.
    /// </summary>
    public sealed class EtlSettingsDto
    {
        public const string SectionName = "Etl";

        /// <summary>
        /// Path to the input CSV file containing taxi trip records.
        /// </summary>
        public string InputCsvPath { get; set; } = string.Empty;

        /// <summary>
        /// Path to the CSV file where removed duplicate rows will be written.
        /// </summary>
        public string DuplicatesCsvPath { get; set; } = string.Empty;

        /// <summary>
        /// Maximum number of valid, non-duplicate trips to accumulate in memory
        /// before invoking the bulk inserter. Reasonable defaults are in the range
        /// of 1 000 - 10 000, depending on the environment.
        /// </summary>
        public int BatchSize { get; set; } = 5_000;

        /// <summary>
        /// Whether to convert timestamps from the input time zone
        /// (see <see cref="InputTimeZoneId"/>) to UTC before inserting into the database.
        /// </summary>
        public bool EnableTimeZoneConversion { get; set; } = true;

        /// <summary>
        /// Time zone identifier of the input data, used when
        /// <see cref="EnableTimeZoneConversion"/> is <c>true</c>.
        /// For example: "Eastern Standard Time".
        /// </summary>
        public string InputTimeZoneId { get; set; } = "Eastern Standard Time";

        /// <summary>
        /// Optional CSV delimiter. If not set, the default delimiter of the CSV reader is used.
        /// </summary>
        public string? CsvDelimiter { get; set; }

        /// <summary>
        /// Optional custom date/time format string for parsing pickup/dropoff timestamps,
        /// if the default parsing logic is not sufficient.
        /// </summary>
        public string? InputDateTimeFormat { get; set; }
    }
}
