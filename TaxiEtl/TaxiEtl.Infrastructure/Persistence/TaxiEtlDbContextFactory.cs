using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;
using TaxiEtl.Infrastructure.Persistence.Options;

namespace TaxiEtl.Infrastructure.Persistence
{
    public sealed class TaxiEtlDbContextFactory
            : IDesignTimeDbContextFactory<TaxiEtlDbContext>
    {
        public TaxiEtlDbContext CreateDbContext(string[] args)
        {
            // Build configuration (same place you keep your connection string)
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables()
                .Build();

            var dbOptions = configuration.GetSection(DatabaseOptions.SectionName).Get<DatabaseOptions>();
            if (string.IsNullOrWhiteSpace(dbOptions?.ConnectionString))
                throw new InvalidOperationException("Database:ConnectionString is not configured.");

            var optionsBuilder = new DbContextOptionsBuilder<TaxiEtlDbContext>();
            optionsBuilder.UseSqlServer(dbOptions.ConnectionString);

            return new TaxiEtlDbContext(optionsBuilder.Options);
        }
    }
}
