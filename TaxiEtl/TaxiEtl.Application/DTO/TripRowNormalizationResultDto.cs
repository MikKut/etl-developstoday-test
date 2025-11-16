using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Application.DTO
{
    /// <summary>
    /// Result of normalizing a parsed trip row into a domain <see cref="Trip"/>.
    /// </summary>
    public sealed class TripRowNormalizationResultDto
    {
        /// <summary>
        /// Indicates whether the normalization step succeeded.
        /// </summary>
        public bool IsSuccess { get; init; }

        /// <summary>
        /// The normalized domain trip entity, available only when <see cref="IsSuccess"/> is true.
        /// </summary>
        public Trip? Trip { get; init; }

        /// <summary>
        /// Human-readable error message describing why normalization failed.
        /// Present only when <see cref="IsSuccess"/> is false.
        /// </summary>
        public string? ErrorMessage { get; init; }

        /// <summary>
        /// Creates a successful normalization result.
        /// </summary>
        public static TripRowNormalizationResultDto Success(Trip trip) =>
            new TripRowNormalizationResultDto
            {
                IsSuccess = true,
                Trip = trip,
                ErrorMessage = null
            };

        /// <summary>
        /// Creates a failed normalization result with the specified error message.
        /// </summary>
        public static TripRowNormalizationResultDto Failure(string errorMessage) =>
            new TripRowNormalizationResultDto
            {
                IsSuccess = false,
                Trip = null,
                ErrorMessage = errorMessage
            };
    }
}
