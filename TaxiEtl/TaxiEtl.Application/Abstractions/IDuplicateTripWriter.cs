using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Writes duplicate trip rows to some output sink (e.g. a CSV file).
    /// </summary>
    public interface IDuplicateTripWriter
    {
        /// <summary>
        /// Writes information about a duplicate raw CSV row to the underlying sink.
        /// The method should be safe to call multiple times and is expected to be
        /// used during a single ETL run.
        /// </summary>
        /// <param name="rawRow">The raw CSV row that has been detected as a duplicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task WriteDuplicateAsync(
            CsvTripRawRowDto rawRow,
            CancellationToken cancellationToken = default);
    }
}
