using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Normalizes a parsed trip row into a domain <see cref="Trip"/>,
    /// applying time zone conversion, enum mappings, and domain invariants.
    /// </summary>
    public interface ITripRowNormalizerService
    {
        /// <summary>
        /// Attempts to normalize the specified parsed row into a domain <see cref="Trip"/>.
        /// On success, returns a result with <see cref="TripRowNormalizationResultDto.IsSuccess"/> set to true
        /// and a non-null <see cref="TripRowNormalizationResultDto.Trip"/>.
        /// On failure, returns a result with <see cref="TripRowNormalizationResultDto.IsSuccess"/> set to false
        /// and a populated <see cref="TripRowNormalizationResultDto.ErrorMessage"/>.
        /// </summary>
        /// <param name="parsedRow">The parsed row to normalize.</param>
        /// <returns>A normalization result describing either success or failure.</returns>
        TripRowNormalizationResultDto Normalize(ParsedTripRowDto parsedRow);
    }
}
