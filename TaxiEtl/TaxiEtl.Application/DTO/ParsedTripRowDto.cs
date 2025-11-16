using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Represents a parsed (but not yet normalized) CSV row with strongly typed values.
    /// Timestamps are still in the input/local time zone and have not been converted to UTC.
    /// </summary>
    public sealed class ParsedTripRowDto
    {
        /// <summary>
        /// 1-based line number in the source CSV file (data row index, excluding header).
        /// Used for diagnostics and error reporting.
        /// </summary>
        public int LineNumber { get; init; }

        /// <summary>
        /// Pickup timestamp in the input time zone (e.g. EST), with unspecified or local kind.
        /// Will be converted to UTC during normalization.
        /// </summary>
        public DateTime PickupLocal { get; init; }

        /// <summary>
        /// Dropoff timestamp in the input time zone (e.g. EST), with unspecified or local kind.
        /// Will be converted to UTC during normalization.
        /// </summary>
        public DateTime DropoffLocal { get; init; }

        public byte PassengerCount { get; init; }

        public decimal TripDistance { get; init; }

        /// <summary>
        /// Raw store-and-forward flag value after basic parsing and normalization,
        /// typically "N" or "Y".
        /// The mapping to the domain enum is performed during normalization.
        /// </summary>
        public string StoreAndFwdFlagRaw { get; init; } = string.Empty;

        public int PULocationId { get; init; }

        public int DOLocationId { get; init; }

        public decimal FareAmount { get; init; }

        public decimal TipAmount { get; init; }
    }
}
