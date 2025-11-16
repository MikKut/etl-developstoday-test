using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Provides bulk insertion of <see cref="Trip"/> entities into a persistent store.
    /// </summary>
    public interface IBulkTripInserterService
    {
        /// <summary>
        /// Inserts the specified trips using a single bulk operation.
        /// </summary>
        /// <param name="trips">
        /// Trips to be inserted. Must not be <c>null</c>. Implementations may treat an empty collection as a no-op.
        /// </param>
        /// <param name="cancellationToken">
        /// Token used to cancel the bulk insert operation.
        /// </param>
        /// <returns>
        /// A task representing the asynchronous operation.
        /// </returns>
        Task InsertBatchAsync(
            IReadOnlyCollection<Trip> trips,
            CancellationToken cancellationToken = default);
    }
}
