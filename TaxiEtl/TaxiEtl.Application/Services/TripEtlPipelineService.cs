using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Application.Services;

/// <summary>
/// Default implementation of the ETL pipeline for taxi trips.
/// Orchestrates CSV reading, parsing, normalization, duplicate detection,
/// batching, and bulk insertion.
/// </summary>
public sealed class TripEtlPipelineService : ITripEtlPipelineService
{
    private readonly ICsvTripReaderService _csvTripReader;
    private readonly ITripRowParserService _tripRowParser;
    private readonly ITripRowNormalizerService _tripRowNormalizer;
    private readonly ITripDuplicateDetectorService _duplicateDetector;
    private readonly IDuplicateTripWriter _duplicateTripWriter;
    private readonly IBulkTripInserterService _bulkTripInserter;
    private readonly EtlSettingsDto _etlSettings;
    private readonly ILogger<TripEtlPipelineService> _logger;

    public TripEtlPipelineService(
        ICsvTripReaderService csvTripReader,
        ITripRowParserService tripRowParser,
        ITripRowNormalizerService tripRowNormalizer,
        ITripDuplicateDetectorService duplicateDetector,
        IDuplicateTripWriter duplicateTripWriter,
        IBulkTripInserterService bulkTripInserter,
        IOptions<EtlSettingsDto> etlOptions,
        ILogger<TripEtlPipelineService> logger)
    {
        _csvTripReader = csvTripReader ?? throw new ArgumentNullException(nameof(csvTripReader));
        _tripRowParser = tripRowParser ?? throw new ArgumentNullException(nameof(tripRowParser));
        _tripRowNormalizer = tripRowNormalizer ?? throw new ArgumentNullException(nameof(tripRowNormalizer));
        _duplicateDetector = duplicateDetector ?? throw new ArgumentNullException(nameof(duplicateDetector));
        _duplicateTripWriter = duplicateTripWriter ?? throw new ArgumentNullException(nameof(duplicateTripWriter));
        _bulkTripInserter = bulkTripInserter ?? throw new ArgumentNullException(nameof(bulkTripInserter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (etlOptions is null)
            throw new ArgumentNullException(nameof(etlOptions));

        _etlSettings = etlOptions.Value ?? throw new ArgumentException(
            "ETL settings must be provided.", nameof(etlOptions));

        if (_etlSettings.BatchSize <= 0)
        {
            throw new ArgumentException(
                "ETL batch size must be a positive integer.",
                nameof(etlOptions));
        }
    }

    /// <inheritdoc />
    public async Task<TripImportStatsDto> RunAsync(
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Starting ETL pipeline. Input CSV: {InputCsvPath}, BatchSize: {BatchSize}",
            _etlSettings.InputCsvPath,
            _etlSettings.BatchSize);

        int totalRowsRead = 0;
        int parsedRows = 0;
        int invalidRows = 0;
        int duplicateRows = 0;
        int insertedRows = 0;
        int duplicatesFileRows = 0;

        var batch = new List<Trip>(_etlSettings.BatchSize);

        await foreach (var rawRow in _csvTripReader.ReadAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            totalRowsRead++;

            // 1. Parse raw CSV row into strongly typed ParsedTripRowDto
            var parseResult = _tripRowParser.Parse(rawRow);
            if (!parseResult.IsSuccess)
            {
                invalidRows++;
                if (!string.IsNullOrWhiteSpace(parseResult.ErrorMessage))
                {
                    _logger.LogWarning(
                        "Line {LineNumber}: parsing failed - {ErrorMessage}",
                        rawRow.LineNumber,
                        parseResult.ErrorMessage);
                }
                else
                {
                    _logger.LogWarning(
                        "Line {LineNumber}: parsing failed with unknown error.",
                        rawRow.LineNumber);
                }

                continue;
            }

            parsedRows++;

            var parsedRow = parseResult.ParsedRow!;
            // 2. Normalize parsed row into domain Trip (UTC timestamps, enums, invariants)
            var normalizeResult = _tripRowNormalizer.Normalize(parsedRow);
            if (!normalizeResult.IsSuccess)
            {
                invalidRows++;
                if (!string.IsNullOrWhiteSpace(normalizeResult.ErrorMessage))
                {
                    _logger.LogWarning(
                        "Line {LineNumber}: normalization failed - {ErrorMessage}",
                        parsedRow.LineNumber,
                        normalizeResult.ErrorMessage);
                }
                else
                {
                    _logger.LogWarning(
                        "Line {LineNumber}: normalization failed with unknown error.",
                        parsedRow.LineNumber);
                }

                continue;
            }

            var trip = normalizeResult.Trip!;
            // 3. Duplicate detection based on (pickup, dropoff, passenger_count)
            //    ITripDuplicateDetector decides whether this is a new or duplicate trip.
            var isNew = _duplicateDetector.TryRegister(trip);
            if (!isNew)
            {
                duplicateRows++;
                    
                try
                {
                    await _duplicateTripWriter
                        .WriteDuplicateAsync(rawRow, cancellationToken)
                        .ConfigureAwait(false);

                    duplicatesFileRows++;
                }
                catch (Exception ex)
                {
                    // We log the error but still treat the row as a duplicate and
                    // do not send it to the database.
                    _logger.LogError(
                        ex,
                        "Failed to write duplicate row for line {LineNumber} to duplicates file.",
                        rawRow.LineNumber);
                }

                continue;
            }

            // 4. Add to current batch for bulk insertion
            batch.Add(trip);

            if (batch.Count >= _etlSettings.BatchSize)
            {
                insertedRows += await FlushBatchAsync(batch, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        // Flush any remaining trips in the last batch
        if (batch.Count > 0)
        {
            insertedRows += await FlushBatchAsync(batch, cancellationToken)
                .ConfigureAwait(false);
        }

        var stats = new TripImportStatsDto
        {
            TotalRowsRead = totalRowsRead,
            ParsedRows = parsedRows,
            InvalidRows = invalidRows,
            DuplicateRows = duplicateRows,
            InsertedRows = insertedRows,
            DuplicatesFileRows = duplicatesFileRows
        };

        _logger.LogInformation(
            "ETL pipeline completed. Total={Total}, Parsed={Parsed}, Invalid={Invalid}, Duplicates={Duplicates}, Inserted={Inserted}, DuplicatesFile={DuplicatesFile}",
            stats.TotalRowsRead,
            stats.ParsedRows,
            stats.InvalidRows,
            stats.DuplicateRows,
            stats.InsertedRows,
            stats.DuplicatesFileRows);

        return stats;
    }

    /// <summary>
    /// Sends the current batch of trips to the bulk inserter and clears the batch on success.
    /// </summary>
    /// <param name="batch">The batch of trips to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows inserted.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="batch"/> is null.</exception>
    private async Task<int> FlushBatchAsync(
        List<Trip> batch,
        CancellationToken cancellationToken)
    {
        if (batch is null)
            throw new ArgumentNullException(nameof(batch));

        if (batch.Count == 0)
            return 0;

        var count = batch.Count;

        _logger.LogDebug(
            "Flushing batch of {BatchCount} trips to bulk inserter.",
            count);

        await _bulkTripInserter
            .InsertBatchAsync(batch, cancellationToken)
            .ConfigureAwait(false);

        batch.Clear();

        return count;
    }
}