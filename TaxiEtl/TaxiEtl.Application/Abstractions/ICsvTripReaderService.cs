using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Abstractions
{
    /// <summary>
    /// Reads raw CSV rows for taxi trips from a source (file, stream, etc.)
    /// and exposes them as a stream of <see cref="CsvTripRawRowDto"/> instances.
    /// </summary>
    public interface ICsvTripReaderService
    {
        /// <summary>
        /// Asynchronously reads raw CSV rows and yields them one by one as
        /// <see cref="CsvTripRawRowDto"/> instances.
        /// 
        /// The method is streaming-friendly and does not load the entire CSV
        /// into memory at once.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// An async sequence of <see cref="CsvTripRawRowDto"/> values representing
        /// data rows in the input CSV (excluding the header row).
        /// </returns>
        IAsyncEnumerable<CsvTripRawRowDto> ReadAsync(
            CancellationToken cancellationToken = default);
    }
}
