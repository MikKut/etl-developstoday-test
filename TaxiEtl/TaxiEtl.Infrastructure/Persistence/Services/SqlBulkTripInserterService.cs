using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Data;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.Constants;
using TaxiEtl.Domain.Entities;
using TaxiEtl.Infrastructure.Persistence.Options;
using TaxiEtl.Infrastructure.Persistence.Schema;

namespace TaxiEtl.Infrastructure.Persistence.Services;

/// <inheritdoc cref="IBulkTripInserterService"/>
public sealed class SqlBulkTripInserterService : IBulkTripInserterService
{
    private readonly DatabaseOptions _dbOptions;
    private readonly ILogger<SqlBulkTripInserterService> _logger;

    private readonly string DestinationTableName;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkTripInserterService"/> class.
    /// </summary>
    /// <param name="dbOptions">
    /// Database-related configuration options, including the connection string
    /// and optional bulk copy settings.
    /// </param>
    /// <param name="logger">
    /// Logger used to emit diagnostic information about the bulk insert process.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="dbOptions"/> or <paramref name="logger"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the resolved <see cref="DatabaseOptions.ConnectionString"/> is
    /// <c>null</c>, empty, or consists only of white-space characters.
    /// </exception>
    public SqlBulkTripInserterService(
        IOptions<DatabaseOptions> dbOptions,
        ILogger<SqlBulkTripInserterService> logger)
    {
        if (dbOptions is null)
            throw new ArgumentNullException(nameof(dbOptions));

        _dbOptions = dbOptions.Value;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (string.IsNullOrWhiteSpace(_dbOptions.ConnectionString))
        {
            throw new ArgumentException("Database connection string must be provided.", nameof(dbOptions));
        }

        DestinationTableName = TripsTable.GetFullName();
    }

    /// <inheritdoc />
    public async Task InsertBatchAsync(
        IReadOnlyCollection<Trip> trips,
        CancellationToken cancellationToken = default)
    {
        if (trips is null)
            throw new ArgumentNullException(nameof(trips));

        if (trips.Count == 0)
        {
            _logger.LogDebug(
                "InsertBatchAsync called with empty trips batch. Skipping bulk insert.");
            return;
        }

        // NOTE:
        // Using a DataTable here is a simple and readable way to prepare a batch
        // for SqlBulkCopy. It materializes only the current batch in memory,
        // not the whole CSV file. For the expected batch sizes in this assignment
        // this is perfectly acceptable.
        //
        // If in the future batch sizes grow significantly or we need to squeeze
        // per-batch allocations even further, this code can be replaced with a
        // streaming DbDataReader over Trip and SqlBulkCopy.WriteToServerAsync(reader).
        var dataTable = BulkSchema.BuildDataTable(trips);

        await using var connection = new SqlConnection(_dbOptions.ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        using var bulkCopy = CreateConfiguredBulkCopy(connection);

        try
        {
            _logger.LogInformation(
                "Starting bulk insert for {RowCount} trips into table {TableName}.",
                trips.Count,
                bulkCopy.DestinationTableName);

            await bulkCopy
                .WriteToServerAsync(dataTable, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Successfully bulk inserted {RowCount} trips into table {TableName}.",
                trips.Count,
                bulkCopy.DestinationTableName);
        }
        catch (Exception ex) when (ex is SqlException || ex is InvalidOperationException)
        {
            _logger.LogError(
                ex,
                "Failed to bulk insert trips batch of size {RowCount} into table {TableName}.",
                trips.Count,
                bulkCopy.DestinationTableName);

            throw;
        }
    }

    private SqlBulkCopy CreateConfiguredBulkCopy(SqlConnection connection)
    {
        var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = DestinationTableName
        };

        if (_dbOptions.BulkCopyTimeoutSeconds.HasValue)
        {
            bulkCopy.BulkCopyTimeout = _dbOptions.BulkCopyTimeoutSeconds.Value;
        }

        if (_dbOptions.BulkCopyBatchSize.HasValue)
        {
            bulkCopy.BatchSize = _dbOptions.BulkCopyBatchSize.Value;
        }

        BulkSchema.AddColumnMappings(bulkCopy);

        return bulkCopy;
    }

    /// <summary>
    /// Contains schema metadata and helper methods used to prepare data for
    /// bulk insertion into the <c>[dbo].[Trips]</c> table.
    /// </summary>
    internal static class BulkSchema
    {
        private static readonly (string Name, Type ClrType)[] Columns =
        {
            (TripFieldNames.PickupDateTime,  typeof(DateTime)),
            (TripFieldNames.DropoffDateTime, typeof(DateTime)),
            (TripFieldNames.PassengerCount,  typeof(byte)),
            (TripFieldNames.TripDistance,    typeof(decimal)),
            (TripFieldNames.StoreAndFwdFlag, typeof(string)),
            (TripFieldNames.PULocationId,    typeof(int)),
            (TripFieldNames.DOLocationId,    typeof(int)),
            (TripFieldNames.FareAmount,      typeof(decimal)),
            (TripFieldNames.TipAmount,       typeof(decimal))
        };

        /// <summary>
        /// Builds an in-memory <see cref="DataTable"/> for the given batch of trips.
        /// </summary>
        /// <remarks>
        /// This approach is suitable for moderate batch sizes, where it is acceptable
        /// to materialize the entire batch in memory before performing the bulk copy.
        /// For significantly larger inputs, a streaming <c>DbDataReader</c>-based approach
        /// may be preferable.
        /// </remarks>
        internal static DataTable BuildDataTable(IReadOnlyCollection<Trip> trips)
        {
            var table = new DataTable()
            {
                MinimumCapacity = trips.Count
            };

            foreach (var (name, clrType) in Columns)
            {
                table.Columns.Add(name, clrType);
            }

            foreach (var trip in trips)
            {
                table.Rows.Add(
                    trip.PickupUtc,
                    trip.DropoffUtc,
                    trip.PassengerCount,
                    trip.TripDistance,
                    trip.StoreAndFwdFlag.ToString(),
                    trip.PULocationId,
                    trip.DOLocationId,
                    trip.FareAmount,
                    trip.TipAmount);
            }

            return table;
        }

        /// <summary>
        /// Adds column mappings to the specified <see cref="SqlBulkCopy"/> instance
        /// based on the schema defined in <see cref="Columns"/>.
        /// </summary>
        internal static void AddColumnMappings(SqlBulkCopy bulkCopy)
        {
            foreach (var (name, _) in Columns)
            {
                bulkCopy.ColumnMappings.Add(name, name);
            }
        }
    }
}