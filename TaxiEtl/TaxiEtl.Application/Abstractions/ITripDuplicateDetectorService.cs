using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Detects duplicate trips within a single ETL run based on a deduplication key
    /// (pickup datetime, dropoff datetime, passenger count).
    /// </summary>
    public interface ITripDuplicateDetectorService
    {
        /// <summary>
        /// Registers the specified trip in the deduplication set and returns
        /// whether it is considered new (non-duplicate) or not.
        /// </summary>
        /// <param name="trip">The trip to check and register.</param>
        /// <returns>
        /// <c>true</c> if this is the first time a trip with this deduplication key
        /// is seen (non-duplicate); <c>false</c> if the trip is considered a duplicate.
        /// </returns>
        bool TryRegister(Trip trip);
    }
}
