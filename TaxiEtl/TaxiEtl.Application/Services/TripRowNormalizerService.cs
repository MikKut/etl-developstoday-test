using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;
using TaxiEtl.Domain.Entities;
using TaxiEtl.Domain.ValueObjects;

namespace TaxiEtl.Application.Services
{
    /// <summary>
    /// Default implementation of <see cref="ITripRowNormalizer"/> that
    /// converts a parsed trip row into a domain <see cref="Trip"/>,
    /// applying time zone conversion, enum mappings, and domain invariants.
    /// </summary>
    public sealed class TripRowNormalizerService : ITripRowNormalizerService
    {
        private readonly ILogger<TripRowNormalizerService> _logger;
        private readonly bool _enableTimeZoneConversion;
        private readonly TimeZoneInfo? _inputTimeZone;

        public TripRowNormalizerService(
            IOptions<EtlSettingsDto> etlOptions,
            ILogger<TripRowNormalizerService> logger)
        {
            if (etlOptions is null)
                throw new ArgumentNullException(nameof(etlOptions));

            var _etlSettings = etlOptions.Value
                             ?? throw new ArgumentException("ETL settings must be provided.", nameof(etlOptions));

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _enableTimeZoneConversion = _etlSettings.EnableTimeZoneConversion;

            if (_enableTimeZoneConversion)
            {
                if (string.IsNullOrWhiteSpace(_etlSettings.InputTimeZoneId))
                {
                    throw new ArgumentException(
                        "InputTimeZoneId must be provided when time zone conversion is enabled.",
                        nameof(etlOptions));
                }

                try
                {
                    _inputTimeZone = TimeZoneInfo.FindSystemTimeZoneById(_etlSettings.InputTimeZoneId);
                }
                catch (TimeZoneNotFoundException ex)
                {
                    throw new ArgumentException(
                        $"Invalid input time zone id '{_etlSettings.InputTimeZoneId}'.",
                        nameof(etlOptions),
                        ex);
                }
                catch (InvalidTimeZoneException ex)
                {
                    throw new ArgumentException(
                        $"Invalid input time zone configuration for '{_etlSettings.InputTimeZoneId}'.",
                        nameof(etlOptions),
                        ex);
                }
            }
            else
            {
                _inputTimeZone = null;
            }
        }

        /// <inheritdoc />
        public TripRowNormalizationResultDto Normalize(ParsedTripRowDto parsedRow)
        {
            if (parsedRow is null)
                throw new ArgumentNullException(nameof(parsedRow));

            var lineNumber = parsedRow.LineNumber;

            // 1. Convert timestamps to UTC (or treat as UTC if conversion is disabled)
            DateTime pickupUtc;
            DateTime dropoffUtc;

            try
            {
                pickupUtc = ToUtc(parsedRow.PickupLocal);
                dropoffUtc = ToUtc(parsedRow.DropoffLocal);
            }
            catch (Exception ex) when (ex is ArgumentException || ex is InvalidTimeZoneException)
            {
                var message = $"failed to convert timestamps to UTC: {ex.Message}";
                _logger.LogWarning(
                    ex,
                    "Line {LineNumber}: {Message}",
                    lineNumber,
                    message);

                return TripRowNormalizationResultDto.Failure(message);
            }

            // 2. Map store_and_fwd_flag raw value ("N"/"Y") to domain enum
            if (!TryMapStoreAndFwdFlag(
                parsedRow.StoreAndFwdFlagRaw,
                lineNumber,
                out var storeAndFwdFlag,
                out var flagError))
            {
                return TripRowNormalizationResultDto.Failure(flagError);
            }

            // 3. Create domain Trip and rely on its invariants (dropoff >= pickup, non-negative amounts, etc.)
            try
            {
                var trip = new Trip(
                    pickupUtc,
                    dropoffUtc,
                    parsedRow.PassengerCount,
                    parsedRow.TripDistance,
                    storeAndFwdFlag,
                    parsedRow.PULocationId,
                    parsedRow.DOLocationId,
                    parsedRow.FareAmount,
                    parsedRow.TipAmount);

                return TripRowNormalizationResultDto.Success(trip);
            }
            catch (Exception ex) when (ex is ArgumentException || ex is InvalidOperationException)
            {
                // Domain-level validation failed; treat as invalid row for ETL purposes.
                var message = $"domain validation failed: {ex.Message}";
                _logger.LogWarning(
                    ex,
                    "Line {LineNumber}: {Message}",
                    lineNumber,
                    message);

                return TripRowNormalizationResultDto.Failure(message);
            }
        }

        private bool TryMapStoreAndFwdFlag(
        string? rawValue,
        int lineNumber,
        out StoreAndFwdFlag result,
        out string error)
        {
            var normalized = rawValue?.Trim().ToUpperInvariant() ?? string.Empty;

            switch (normalized)
            {
                case "N":
                    result = StoreAndFwdFlag.No;
                    error = string.Empty;
                    return true;

                case "Y":
                    result = StoreAndFwdFlag.Yes;
                    error = string.Empty;
                    return true;

                default:
                    error = $"invalid store_and_fwd_flag value '{normalized}'. Expected 'N' or 'Y'.";
                    _logger.LogWarning(
                        "Line {LineNumber}: {Message}",
                        lineNumber,
                        error);
                    result = default;
                    return false;
            }
        }


        /// <summary>
        /// Converts a local timestamp (in the input time zone, or already UTC)
        /// into a UTC <see cref="DateTime"/> according to ETL settings.
        /// </summary>
        private DateTime ToUtc(DateTime localTime)
        {
            if (!_enableTimeZoneConversion)
            {
                if (localTime.Kind == DateTimeKind.Utc)
                    return localTime;

                return DateTime.SpecifyKind(localTime, DateTimeKind.Utc);
            }

            if (_inputTimeZone is null)
            {
                throw new InvalidOperationException(
                    "Input time zone is not configured but time zone conversion is enabled.");
            }

            return TimeZoneInfo.ConvertTimeToUtc(localTime, _inputTimeZone);
        }
    }
}
