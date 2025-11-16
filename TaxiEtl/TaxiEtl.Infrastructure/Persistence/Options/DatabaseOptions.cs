namespace TaxiEtl.Infrastructure.Persistence.Options
{
    /// <summary>
    /// Database-related configuration options used by the ETL infrastructure:
    /// connection string and optional tuning parameters for bulk operations.
    /// 
    /// Typically bound from the "Database" section in appsettings.json.
    /// </summary>
    public sealed class DatabaseOptions
    {
        /// <summary>
        /// Configuration section name to bind these options from.
        /// </summary>
        public const string SectionName = "Database";

        /// <summary>
        /// Connection string for the SQL Server database used by the ETL process.
        /// This value is required.
        /// </summary>
        public string ConnectionString { get; set; } = string.Empty;

        /// <summary>
        /// Optional timeout, in seconds, for SqlBulkCopy operations.
        /// If not set, the provider's default timeout will be used.
        /// </summary>
        public int? BulkCopyTimeoutSeconds { get; set; }

        /// <summary>
        /// Optional batch size for SqlBulkCopy operations.
        /// If specified, it will be assigned to <c>SqlBulkCopy.BatchSize</c>.
        /// If not set, the default batch size will be used.
        /// </summary>
        public int? BulkCopyBatchSize { get; set; }
    }
}
