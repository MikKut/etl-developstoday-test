using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Represents a single raw CSV row for a taxi trip, as read from the input file,
    /// before any trimming, validation, or type conversion has been applied.
    /// All values (except <see cref="LineNumber"/>) are kept as strings.
    /// </summary>
    public sealed class CsvTripRawRowDto
    {
        /// <summary>
        /// 1-based line number in the source CSV file (including header if applicable).
        /// Used for diagnostics and error reporting.
        /// </summary>
        public int LineNumber { get; init; }

        // Raw values as they appear in the CSV (after basic reader-level handling).
        public string? TpepPickupDatetime { get; init; }
        public string? TpepDropoffDatetime { get; init; }
        public string? PassengerCount { get; init; }
        public string? TripDistance { get; init; }
        public string? StoreAndFwdFlag { get; init; }
        public string? PULocationId { get; init; }
        public string? DOLocationId { get; init; }
        public string? FareAmount { get; init; }
        public string? TipAmount { get; init; }
    }
}
