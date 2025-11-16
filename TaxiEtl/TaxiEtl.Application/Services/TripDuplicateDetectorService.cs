using TaxiEtl.Application.Abstractions;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Application.Services
{

    /// <summary>
    /// In-memory implementation of <see cref="ITripDuplicateDetectorService"/> using
    /// a <see cref="HashSet{T}"/> of deduplication keys based on
    /// (pickup datetime, dropoff datetime, passenger count).
    /// NOTE:
    /// This approach is intentionally simple and works well for the provided sample
    /// CSV and small/medium files. For very large inputs (e.g. 10GB) the number of
    /// unique keys may be high, which can put pressure on memory. In such scenarios
    /// deduplication can be moved closer to the database (staging table + SQL
    /// ROW_NUMBER/PARTITION BY) or combined with a UNIQUE index.
    /// </summary>
    public sealed class TripDuplicateDetectorService : ITripDuplicateDetectorService
    {
        private readonly HashSet<DuplicateKey> _seenKeys;
        /// <summary>
        /// Initializes a new instance of the <see cref="TripDuplicateDetectorService"/> class.
        /// </summary>
        /// <param name="initialCapacity">
        /// Optional initial capacity hint for the internal HashSet.
        /// This can reduce reallocations for large imports.
        /// </param>
        public TripDuplicateDetectorService(int? initialCapacity = null)
        {
            _seenKeys = initialCapacity.HasValue
                ? new HashSet<DuplicateKey>(initialCapacity.Value)
                : new HashSet<DuplicateKey>();
        }

        /// <inheritdoc />
        public bool TryRegister(Trip trip)
        {
            if (trip is null)
                throw new ArgumentNullException(nameof(trip));

            var key = new DuplicateKey(
                trip.PickupUtc,
                trip.DropoffUtc,
                trip.PassengerCount);

            // HashSet.Add returns true if the element was added (i.e. not present before),
            // and false if it was already contained (i.e. this is a duplicate).
            return _seenKeys.Add(key);
        }

        /// <summary>
        /// Represents a deduplication key for a trip, based on the combination of
        /// pickup datetime, dropoff datetime, and passenger count.
        /// </summary>
        private readonly record struct DuplicateKey(
            DateTime PickupUtc,
            DateTime DropoffUtc,
            byte PassengerCount);
    }
}
