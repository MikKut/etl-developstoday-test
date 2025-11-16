using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.Constants;
using TaxiEtl.Application.DTO;
using TaxiEtl.Infrastructure.Persistence.Schema;

namespace TaxiEtl.Infrastructure.Persistence.Services
{
    /// <summary>
    /// CSV-based implementation of <see cref="IDuplicateTripWriter"/> that writes
    /// duplicate rows into a configured duplicates.csv file.
    /// </summary>
    public sealed class CsvDuplicateTripWriter : IDuplicateTripWriter
    {
        private readonly EtlSettingsDto _etlSettings;
        private readonly ILogger<CsvDuplicateTripWriter> _logger;

        private static readonly string[] HeaderColumns =
{
            "LineNumber",
            TripFieldNames.PickupDateTime,
            TripFieldNames.DropoffDateTime,
            TripFieldNames.PassengerCount,
            TripFieldNames.TripDistance,
            TripFieldNames.StoreAndFwdFlag,
            TripFieldNames.PULocationId,
            TripFieldNames.DOLocationId,
            TripFieldNames.FareAmount,
            TripFieldNames.TipAmount
        };

        public CsvDuplicateTripWriter(
            IOptions<EtlSettingsDto> etlOptions,
            ILogger<CsvDuplicateTripWriter> logger)
        {
            if (etlOptions is null)
                throw new ArgumentNullException(nameof(etlOptions));

            _etlSettings = etlOptions.Value
                             ?? throw new ArgumentException("ETL settings must be provided.", nameof(etlOptions));

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(_etlSettings.DuplicatesCsvPath))
            {
                throw new ArgumentException(
                    "EtlSettings.DuplicatesCsvPath must be provided.",
                    nameof(etlOptions));
            }
        }

        /// <inheritdoc />
        public async Task WriteDuplicateAsync(
            CsvTripRawRowDto rawRow,
            CancellationToken cancellationToken = default)
        {
            if (rawRow is null)
                throw new ArgumentNullException(nameof(rawRow));

            var path = _etlSettings.DuplicatesCsvPath;

            try
            {
                // Ensure the directory exists
                var directory = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var fileExists = File.Exists(path);

                // Open file in append mode
                await using var stream = new FileStream(
                    path,
                    mode: FileMode.Append,
                    access: FileAccess.Write,
                    share: FileShare.Read,
                    bufferSize: 4096,
                    options: FileOptions.Asynchronous | FileOptions.SequentialScan);

                await using var writer = new StreamWriter(
                    stream,
                    new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

                // If the file did not exist before, write a header first.
                if (!fileExists)
                {
                    var headerLine = BuildHeaderLine();
                    await writer.WriteLineAsync(headerLine.AsMemory(), cancellationToken)
                                .ConfigureAwait(false);
                }

                var dataLine = BuildDataLine(rawRow);
                await writer.WriteLineAsync(dataLine.AsMemory(), cancellationToken)
                            .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                _logger.LogError(
                    ex,
                    "Failed to write duplicate row for line {LineNumber} to duplicates CSV file '{Path}'.",
                    rawRow.LineNumber,
                    path);
            }
        }

        private static string BuildHeaderLine()
        {
            return string.Join(",", Array.ConvertAll(HeaderColumns, ToCsvField));
        }

        private static string BuildDataLine(CsvTripRawRowDto row)
        {
            var fields = new[]
            {
            row.LineNumber.ToString(CultureInfo.InvariantCulture),
            row.TpepPickupDatetime,
            row.TpepDropoffDatetime,
            row.PassengerCount,
            row.TripDistance,
            row.StoreAndFwdFlag,
            row.PULocationId,
            row.DOLocationId,
            row.FareAmount,
            row.TipAmount
        };

            return string.Join(",", Array.ConvertAll(fields, ToCsvField));
        }

        /// <summary>
        /// Escapes a value as a CSV field, adding quotes if necessary and doubling inner quotes.
        /// </summary>
        private static string ToCsvField(string? value)
        {
            if (value is null)
            {
                return string.Empty;
            }

            var needsQuotes =
                value.Contains(',') ||
                value.Contains('"') ||
                value.Contains('\r') ||
                value.Contains('\n');

            if (!needsQuotes)
            {
                return value;
            }

            var escaped = value.Replace("\"", "\"\"");
            return $"\"{escaped}\"";
        }
    }
}
