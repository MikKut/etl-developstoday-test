using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.EntityFrameworkCore;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;
using TaxiEtl.Infrastructure.Persistence;
using TaxiEtl.Infrastructure.Persistence.Options;
using TaxiEtl.Infrastructure.Persistence.Services;

namespace TaxiEtl.Infrastructure
{
    /// <summary>
    /// Extension methods for registering Infrastructure-layer services
    /// (database, file system, CSV readers/writers, bulk insert, etc.).
    /// </summary>
    public static class InfrastructureServiceCollectionExtensions
    {
        /// <summary>
        /// Registers Infrastructure-layer services and configures options
        /// using the provided <see cref="IConfiguration"/>.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configuration">The application configuration root.</param>
        public static IServiceCollection AddInfrastructure(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            if (services is null)
                throw new ArgumentNullException(nameof(services));
            if (configuration is null)
                throw new ArgumentNullException(nameof(configuration));

            services.Configure<EtlSettingsDto>(
                configuration.GetSection(EtlSettingsDto.SectionName));
            services.Configure<DatabaseOptions>(
                configuration.GetSection(DatabaseOptions.SectionName));

            var dbOptions = configuration
                .GetSection(DatabaseOptions.SectionName)
                .Get<DatabaseOptions>();

            if (string.IsNullOrWhiteSpace(dbOptions?.ConnectionString))
                throw new InvalidOperationException("Database:ConnectionString is not configured.");

            services.AddDbContext<TaxiEtlDbContext>((sp, options) =>
            {
                var dbOptions = sp.GetRequiredService<IOptions<DatabaseOptions>>().Value;

                if (string.IsNullOrWhiteSpace(dbOptions.ConnectionString))
                {
                    throw new InvalidOperationException(
                        "Database connection string is not configured. " +
                        "Please ensure DatabaseOptions.ConnectionString is set.");
                }

                options.UseSqlServer(
                    dbOptions.ConnectionString,
                    sql =>
                    {
                        sql.EnableRetryOnFailure();
                    });
            });

            services.AddScoped<ICsvTripReaderService, CsvTripReaderService>();
            services.AddScoped<IDuplicateTripWriter, CsvDuplicateTripWriter>();
            services.AddScoped<IBulkTripInserterService, SqlBulkTripInserterService>();

            return services;
        }
    }
}
