using System.Globalization;
using Microsoft.Extensions.Options;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;

namespace TaxiEtl.Application.Services
{
    /// <summary>
    /// Default implementation of <see cref="ITripRowParserService"/> that converts
    /// raw CSV string values into strongly typed <see cref="ParsedTripRowDto"/> instances.
    /// </summary>
    public sealed class TripRowParserService : ITripRowParserService
    {
        private readonly EtlSettingsDto _etlSettings;

        public TripRowParserService(IOptions<EtlSettingsDto> etlOptions)
        {
            if (etlOptions is null)
                throw new ArgumentNullException(nameof(etlOptions));

            _etlSettings = etlOptions.Value
                             ?? throw new ArgumentException("ETL settings must be provided.", nameof(etlOptions));
        }

        /// <inheritdoc />
        public TripRowParseResultDto Parse(CsvTripRawRowDto rawRow)
        {
            if (rawRow is null)
                throw new ArgumentNullException(nameof(rawRow));

            var lineNumber = rawRow.LineNumber;

            // 1. Pickup datetime
            if (!TryParseDateTime(
                    rawRow.TpepPickupDatetime,
                    fieldName: "tpep_pickup_datetime",
                    out var pickupLocal,
                    out var error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid tpep_pickup_datetime value: {error}");
            }

            // 2. Dropoff datetime
            if (!TryParseDateTime(
                    rawRow.TpepDropoffDatetime,
                    fieldName: "tpep_dropoff_datetime",
                    out var dropoffLocal,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid tpep_dropoff_datetime value: {error}");
            }

            // 3. Passenger count (byte)
            if (!TryParseByte(
                    rawRow.PassengerCount,
                    fieldName: "passenger_count",
                    minValueInclusive: 0,
                    out var passengerCount,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid passenger_count value: {error}");
            }

            // 4. Trip distance (decimal)
            if (!TryParseDecimal(
                    rawRow.TripDistance,
                    fieldName: "trip_distance",
                    minValueInclusive: 0m,
                    out var tripDistance,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid trip_distance value: {error}");
            }

            // 5. Store-and-forward flag (raw, normalized to upper-case, no semantics yet)
            if (!TryParseNonEmptyString(
                    rawRow.StoreAndFwdFlag,
                    fieldName: "store_and_fwd_flag",
                    out var storeAndFwdFlagRaw,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid store_and_fwd_flag value: {error}");
            }

            // Normalization for later enum mapping: trim + upper-case.
            storeAndFwdFlagRaw = storeAndFwdFlagRaw.Trim().ToUpperInvariant();

            // 6. PULocationID (int)
            if (!TryParseInt(
                    rawRow.PULocationId,
                    fieldName: "PULocationID",
                    minValueInclusive: 0,
                    out var puLocationId,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid PULocationID value: {error}");
            }

            // 7. DOLocationID (int)
            if (!TryParseInt(
                    rawRow.DOLocationId,
                    fieldName: "DOLocationID",
                    minValueInclusive: 0,
                    out var doLocationId,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid DOLocationID value: {error}");
            }

            // 8. Fare amount (decimal)
            if (!TryParseDecimal(
                    rawRow.FareAmount,
                    fieldName: "fare_amount",
                    minValueInclusive: 0m,
                    out var fareAmount,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid fare_amount value: {error}");
            }

            // 9. Tip amount (decimal)
            if (!TryParseDecimal(
                    rawRow.TipAmount,
                    fieldName: "tip_amount",
                    minValueInclusive: 0m,
                    out var tipAmount,
                    out error))
            {
                return TripRowParseResultDto.Failure(
                    $"invalid tip_amount value: {error}");
            }

            var parsed = new ParsedTripRowDto
            {
                LineNumber = lineNumber,
                PickupLocal = pickupLocal,
                DropoffLocal = dropoffLocal,
                PassengerCount = passengerCount,
                TripDistance = tripDistance,
                StoreAndFwdFlagRaw = storeAndFwdFlagRaw,
                PULocationId = puLocationId,
                DOLocationId = doLocationId,
                FareAmount = fareAmount,
                TipAmount = tipAmount
            };

            return TripRowParseResultDto.Success(parsed);
        }

        #region Helpers

        private bool TryParseDateTime(
            string? rawValue,
            string fieldName,
            out DateTime result,
            out string error)
        {
            result = default;

            if (string.IsNullOrWhiteSpace(rawValue))
            {
                error = "value is missing or empty.";
                return false;
            }

            var trimmed = rawValue.Trim();
            var format = _etlSettings.InputDateTimeFormat;

            bool success;
            if (!string.IsNullOrWhiteSpace(format))
            {
                success = DateTime.TryParseExact(
                    trimmed,
                    format,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out var parsed);
                if (!success)
                {
                    error = $"'{trimmed}' does not match expected format '{format}'.";
                    return false;
                }

                // Keep kind unspecified; normalization will decide how to treat it.
                result = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
                error = string.Empty;
                return true;
            }

            success = DateTime.TryParse(
                trimmed,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var parsedDefault);

            if (!success)
            {
                error = $"'{trimmed}' is not a valid date/time value.";
                return false;
            }

            result = DateTime.SpecifyKind(parsedDefault, DateTimeKind.Unspecified);
            error = string.Empty;
            return true;
        }

        private static bool TryParseByte(
            string? rawValue,
            string fieldName,
            byte minValueInclusive,
            out byte result,
            out string error)
        {
            result = default;

            if (string.IsNullOrWhiteSpace(rawValue))
            {
                error = "value is missing or empty.";
                return false;
            }

            var trimmed = rawValue.Trim();

            if (!byte.TryParse(
                    trimmed,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var parsed))
            {
                error = $"'{trimmed}' is not a valid integer.";
                return false;
            }

            if (parsed < minValueInclusive)
            {
                error = $"'{parsed}' is less than the minimum allowed value {minValueInclusive}.";
                return false;
            }

            result = parsed;
            error = string.Empty;
            return true;
        }

        private static bool TryParseInt(
            string? rawValue,
            string fieldName,
            int minValueInclusive,
            out int result,
            out string error)
        {
            result = default;

            if (string.IsNullOrWhiteSpace(rawValue))
            {
                error = "value is missing or empty.";
                return false;
            }

            var trimmed = rawValue.Trim();

            if (!int.TryParse(
                    trimmed,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var parsed))
            {
                error = $"'{trimmed}' is not a valid integer.";
                return false;
            }

            if (parsed < minValueInclusive)
            {
                error = $"'{parsed}' is less than the minimum allowed value {minValueInclusive}.";
                return false;
            }

            result = parsed;
            error = string.Empty;
            return true;
        }

        private static bool TryParseDecimal(
            string? rawValue,
            string fieldName,
            decimal minValueInclusive,
            out decimal result,
            out string error)
        {
            result = default;

            if (string.IsNullOrWhiteSpace(rawValue))
            {
                error = "value is missing or empty.";
                return false;
            }

            var trimmed = rawValue.Trim();

            if (!decimal.TryParse(
                    trimmed,
                    NumberStyles.Float | NumberStyles.AllowThousands,
                    CultureInfo.InvariantCulture,
                    out var parsed))
            {
                error = $"'{trimmed}' is not a valid decimal number.";
                return false;
            }

            if (parsed < minValueInclusive)
            {
                error = $"'{parsed}' is less than the minimum allowed value {minValueInclusive}.";
                return false;
            }

            result = parsed;
            error = string.Empty;
            return true;
        }

        private static bool TryParseNonEmptyString(
            string? rawValue,
            string fieldName,
            out string result,
            out string error)
        {
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                result = string.Empty;
                error = "value is missing or empty.";
                return false;
            }

            result = rawValue.Trim();
            error = string.Empty;
            return true;
        }

        #endregion
    }
}
