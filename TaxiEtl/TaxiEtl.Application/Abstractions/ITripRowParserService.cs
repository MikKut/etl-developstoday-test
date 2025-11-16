using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Parses a raw CSV trip row with string values into a strongly typed
    /// <see cref="ParsedTripRowDto"/>, performing trimming and format validation.
    /// </summary>
    public interface ITripRowParserService
    {
        /// <summary>
        /// Attempts to parse the specified raw CSV row into a strongly typed
        /// <see cref="ParsedTripRowDto"/>.
        /// On success, returns a result with <see cref="TripRowParseResultDto.IsSuccess"/> set to true
        /// and a non-null <see cref="TripRowParseResultDto.ParsedRow"/>.
        /// On failure, returns a result with <see cref="TripRowParseResultDto.IsSuccess"/> set to false
        /// and a populated <see cref="TripRowParseResultDto.ErrorMessage"/>.
        /// </summary>
        /// <param name="rawRow">The raw CSV row to parse.</param>
        /// <returns>A parsing result describing either success or failure.</returns>
        TripRowParseResultDto Parse(CsvTripRawRowDto rawRow);
    }
}
