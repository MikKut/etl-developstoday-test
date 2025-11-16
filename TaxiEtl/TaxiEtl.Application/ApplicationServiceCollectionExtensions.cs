using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;
using TaxiEtl.Application.Services;

namespace TaxiEtl.Application
{
    /// <summary>
    /// Extension methods for registering Application-layer services.
    /// </summary>
    public static class ApplicationServiceCollectionExtensions
    {
        /// <summary>
        /// Registers Application-layer services (use cases, parsers, normalizers, etc.).
        /// </summary>
        public static IServiceCollection AddApplication(this IServiceCollection services)
        {
            if (services is null)
                throw new ArgumentNullException(nameof(services));

            // High-level ETL use case
            services.AddScoped<ITripEtlPipelineService, TripEtlPipelineService>();
            services.AddScoped<ITripDuplicateDetectorService, TripDuplicateDetectorService>();
            services.AddScoped<ITripRowParserService, TripRowParserService>();
            services.AddScoped<ITripRowNormalizerService, TripRowNormalizerService>();

            // In-memory duplicate detection (per ETL run)
            services.AddScoped<ITripDuplicateDetectorService>(sp =>
            {
                var etlOptions = sp.GetRequiredService<IOptions<EtlSettingsDto>>().Value;
                int? initialCapacity = etlOptions.BatchSize > 0
                    ? etlOptions.BatchSize
                    : null;

                return new TripDuplicateDetectorService(initialCapacity);
            });

            return services;
        }
    }
}
