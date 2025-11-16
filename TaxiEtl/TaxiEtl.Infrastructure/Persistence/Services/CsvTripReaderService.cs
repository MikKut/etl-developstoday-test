using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.Constants;
using TaxiEtl.Application.DTO;
using TaxiEtl.Infrastructure.Persistence.Schema;

namespace TaxiEtl.Infrastructure.Persistence.Services
{
    /// <summary>
    /// File-based implementation of <see cref="ICsvTripReaderService"/> that reads
    /// taxi trip data from a CSV file specified in <see cref="EtlSettingsDto"/>.
    /// </summary>
    public class CsvTripReaderService : ICsvTripReaderService
    {
        private readonly EtlSettingsDto _etlSettings;
        private readonly ILogger<CsvTripReaderService> _logger;
        private readonly string _csvPath;
        private const int BufferSize = 64 * 1024;
        private const char Delimiter = ',';

        public CsvTripReaderService(
            IOptions<EtlSettingsDto> etlOptions,
            ILogger<CsvTripReaderService> logger)
        {
            if (etlOptions is null)
                throw new ArgumentNullException(nameof(etlOptions));

            _etlSettings = etlOptions.Value
                             ?? throw new ArgumentException("ETL settings must be provided.", nameof(etlOptions));

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(_etlSettings.InputCsvPath))
            {
                throw new ArgumentException(
                    "ETL InputCsvPath must be provided.",
                    nameof(etlOptions));
            }

            _csvPath = ResolvePath(_etlSettings.InputCsvPath);
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<CsvTripRawRowDto> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Opening input CSV file: {_csvPath}", _csvPath);

            if (!File.Exists(_csvPath))
            {
                throw new FileNotFoundException(
                    "Input CSV file not found: '{_csvPath}'.",
                    _csvPath);
            }

            var delimiter = ResolveDelimiter(_etlSettings.CsvDelimiter);

            await using var stream = new FileStream(
                _csvPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: BufferSize,
                options: FileOptions.Asynchronous | FileOptions.SequentialScan);

            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync().ConfigureAwait(false);
            if (headerLine is null)
            {
                throw new InvalidDataException("Input CSV file is empty or missing header row.");
            }

            var columnIndexByName = ParseHeader(headerLine, delimiter);
            var columnMap = CreateColumnMap(columnIndexByName);

            _logger.LogInformation(
                "CSV header parsed successfully. Starting to stream data rows from {_csvPath}.",
                _csvPath);

            int dataRowNumber = 0;

            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var line = await reader.ReadLineAsync().ConfigureAwait(false);
                    if (line is null)
                    {
                        break; // EOF
                    }

                    if (string.IsNullOrWhiteSpace(line))
                    {
                        // Skip completely blank lines without incrementing dataRowNumber.
                        continue;
                    }

                    dataRowNumber++;
                    var rawRow = ParseRow(delimiter, columnMap, dataRowNumber, line);
                    yield return rawRow;
                }
            }
            finally
            {
                _logger.LogInformation(
                    "Finished streaming data rows from input CSV file: {_csvPath}. Total data rows read: {dataRowNumber}.",
                    _csvPath,
                    dataRowNumber);
            }
        }

        private static CsvTripRawRowDto ParseRow(char delimiter, ColumnMap columnMap, int dataRowNumber, string line)
        {
            var values = line.Split(delimiter);

            string? GetValue(int index) =>
                index >= 0 && index < values.Length
                    ? values[index]
                    : null;

            var rawRow = new CsvTripRawRowDto
            {
                LineNumber = dataRowNumber,
                TpepPickupDatetime = GetValue(columnMap.PickupIndex),
                TpepDropoffDatetime = GetValue(columnMap.DropoffIndex),
                PassengerCount = GetValue(columnMap.PassengerCountIndex),
                TripDistance = GetValue(columnMap.TripDistanceIndex),
                StoreAndFwdFlag = GetValue(columnMap.StoreAndFwdFlagIndex),
                PULocationId = GetValue(columnMap.PuLocationIdIndex),
                DOLocationId = GetValue(columnMap.DoLocationIdIndex),
                FareAmount = GetValue(columnMap.FareAmountIndex),
                TipAmount = GetValue(columnMap.TipAmountIndex)
            };
            return rawRow;
        }

        private string ResolvePath(string configuredPath)
        {
            if (string.IsNullOrWhiteSpace(configuredPath))
                throw new ArgumentException("InputCsvPath must be provided.", nameof(configuredPath));

            if (Path.IsPathRooted(configuredPath))
                return configuredPath;

            return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, configuredPath));
        }

        private char ResolveDelimiter(string? configuredDelimiter)
        {
            if (string.IsNullOrEmpty(configuredDelimiter))
            {
                return Delimiter;
            }

            return configuredDelimiter[0];
        }

        private Dictionary<string, int> ParseHeader(string headerLine, char delimiter)
        {
            var columns = headerLine.Split(delimiter);
            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < columns.Length; i++)
            {
                var name = columns[i].Trim();
                if (name.Length == 0)
                {
                    continue;
                }

                // If there are duplicate column names, keep the first occurrence.
                if (!dict.ContainsKey(name))
                {
                    dict[name] = i;
                }
            }

            return dict;
        }

        internal ColumnMap CreateColumnMap(Dictionary<string, int> columnIndexByName)
        {
            int pickupIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.PickupDateTime);
            int dropoffIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.DropoffDateTime);
            int passengerIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.PassengerCount);
            int distanceIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.TripDistance);
            int flagIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.StoreAndFwdFlag);
            int puIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.PULocationId);
            int doIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.DOLocationId);
            int fareIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.FareAmount);
            int tipIndex = GetRequiredIndex(columnIndexByName, TripFieldNames.TipAmount);

            return new ColumnMap(
                pickupIndex,
                dropoffIndex,
                passengerIndex,
                distanceIndex,
                flagIndex,
                puIndex,
                doIndex,
                fareIndex,
                tipIndex);
        }

        private static int GetRequiredIndex(
            IReadOnlyDictionary<string, int> columnIndexByName,
            string columnName)
        {
            if (!columnIndexByName.TryGetValue(columnName, out var index))
            {
                throw new InvalidDataException(
                    $"Input CSV is missing required column '{columnName}'.");
            }

            return index;
        }

        /// <summary>
        /// Holds the indices of required CSV columns for efficient lookup.
        /// </summary>
        internal readonly struct ColumnMap
        {
            public ColumnMap(
                int pickupIndex,
                int dropoffIndex,
                int passengerCountIndex,
                int tripDistanceIndex,
                int storeAndFwdFlagIndex,
                int puLocationIdIndex,
                int doLocationIdIndex,
                int fareAmountIndex,
                int tipAmountIndex)
            {
                PickupIndex = pickupIndex;
                DropoffIndex = dropoffIndex;
                PassengerCountIndex = passengerCountIndex;
                TripDistanceIndex = tripDistanceIndex;
                StoreAndFwdFlagIndex = storeAndFwdFlagIndex;
                PuLocationIdIndex = puLocationIdIndex;
                DoLocationIdIndex = doLocationIdIndex;
                FareAmountIndex = fareAmountIndex;
                TipAmountIndex = tipAmountIndex;
            }

            public int PickupIndex { get; }
            public int DropoffIndex { get; }
            public int PassengerCountIndex { get; }
            public int TripDistanceIndex { get; }
            public int StoreAndFwdFlagIndex { get; }
            public int PuLocationIdIndex { get; }
            public int DoLocationIdIndex { get; }
            public int FareAmountIndex { get; }
            public int TipAmountIndex { get; }
        }
    }
}