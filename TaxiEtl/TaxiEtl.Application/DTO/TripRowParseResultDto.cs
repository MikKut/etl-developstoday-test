using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Result of parsing a raw CSV row into a strongly typed <see cref="ParsedTripRowDto"/>.
    /// </summary>
    public sealed class TripRowParseResultDto
    {
        /// <summary>
        /// Indicates whether the parsing step succeeded.
        /// </summary>
        public bool IsSuccess { get; init; }

        /// <summary>
        /// The parsed row with strongly typed values, available only when
        /// <see cref="IsSuccess"/> is true.
        /// </summary>
        public ParsedTripRowDto? ParsedRow { get; init; }

        /// <summary>
        /// Human-readable error message describing why parsing failed.
        /// Present only when <see cref="IsSuccess"/> is false.
        /// </summary>
        public string? ErrorMessage { get; init; }

        /// <summary>
        /// Creates a successful parsing result.
        /// </summary>
        public static TripRowParseResultDto Success(ParsedTripRowDto parsedRow) =>
            new TripRowParseResultDto
            {
                IsSuccess = true,
                ParsedRow = parsedRow,
                ErrorMessage = null
            };

        /// <summary>
        /// Creates a failed parsing result with the specified error message.
        /// </summary>
        public static TripRowParseResultDto Failure(string errorMessage) =>
            new TripRowParseResultDto
            {
                IsSuccess = false,
                ParsedRow = null,
                ErrorMessage = errorMessage
            };
    }
}
