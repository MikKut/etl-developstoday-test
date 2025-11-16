using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Abstractions
{
    public interface ITripEtlPipelineService
    {
        /// <summary>
        /// Runs the ETL process: reads the CSV, parses and normalizes rows,
        /// detects duplicates, performs bulk inserts, and returns aggregated statistics.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Aggregated ETL statistics.</returns>
        Task<TripImportStatsDto> RunAsync(CancellationToken cancellationToken = default);
    }
}
