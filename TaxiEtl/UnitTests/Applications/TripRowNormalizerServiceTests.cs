
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework.Legacy;
using TaxiEtl.Application.DTO;
using TaxiEtl.Application.Services;
using TaxiEtl.Domain.ValueObjects;

namespace TaxiEtl.Tests.Application
{
    [TestFixture]
    public class TripRowNormalizerServiceTests
    {
        private Mock<ILogger<TripRowNormalizerService>> _loggerMock = null!;

        [SetUp]
        public void SetUp()
        {
            _loggerMock = new Mock<ILogger<TripRowNormalizerService>>();
        }

        private TripRowNormalizerService CreateService(EtlSettingsDto settings)
        {
            var options = Options.Create(settings);
            return new TripRowNormalizerService(options, _loggerMock.Object);
        }

        // -------------------------
        // Constructor tests
        // -------------------------

        [Test]
        public void Ctor_NullOptions_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(
                () => new TripRowNormalizerService(null!, _loggerMock.Object));
        }

        [Test]
        public void Ctor_NullLogger_ThrowsArgumentNullException()
        {
            var options = Options.Create(new EtlSettingsDto());
            Assert.Throws<ArgumentNullException>(
                () => new TripRowNormalizerService(options, null!));
        }

        [Test]
        public void Ctor_EnableTimeZoneConversionTrue_WithoutTimeZoneId_ThrowsArgumentException()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = true,
                InputTimeZoneId = null
            };

            // Act + Assert
            var ex = Assert.Throws<ArgumentException>(
                () => CreateService(settings));

            StringAssert.Contains("InputTimeZoneId must be provided", ex!.Message);
        }

        [Test]
        public void Ctor_InvalidTimeZoneId_ThrowsArgumentException()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = true,
                InputTimeZoneId = "This/TimeZone/DoesNotExist"
            };

            // Act + Assert
            var ex = Assert.Throws<ArgumentException>(
                () => CreateService(settings));

            StringAssert.Contains("Invalid input time zone id", ex!.Message);
        }

        // -------------------------
        // Normalize: happy path
        // -------------------------

        [Test]
        public void Normalize_WhenConversionDisabled_TreatsLocalAsUtcAndMapsFlag()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = false
            };
            var service = CreateService(settings);

            var pickupLocal = new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Unspecified);
            var dropoffLocal = new DateTime(2023, 1, 1, 10, 15, 0, DateTimeKind.Unspecified);

            var parsed = new ParsedTripRowDto
            {
                LineNumber = 1,
                PickupLocal = pickupLocal,
                DropoffLocal = dropoffLocal,
                PassengerCount = 2,
                TripDistance = 3.5m,
                StoreAndFwdFlagRaw = " y ",
                PULocationId = 100,
                DOLocationId = 200,
                FareAmount = 15.0m,
                TipAmount = 3.0m
            };

            // Act
            var result = service.Normalize(parsed);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Trip, Is.Not.Null);
            Assert.That(result.ErrorMessage, Is.Null.Or.Empty);

            var trip = result.Trip!;
            Assert.That(trip.PickupUtc.Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That(trip.DropoffUtc.Kind, Is.EqualTo(DateTimeKind.Utc));

            // Для режиму без конверсії ми просто "прикручуємо" Utc-kind:
            Assert.That(trip.PickupUtc, Is.EqualTo(DateTime.SpecifyKind(pickupLocal, DateTimeKind.Utc)));
            Assert.That(trip.DropoffUtc, Is.EqualTo(DateTime.SpecifyKind(dropoffLocal, DateTimeKind.Utc)));

            // Перевіримо мапінг прапорця
            Assert.That(trip.StoreAndFwdFlag, Is.EqualTo(StoreAndFwdFlag.Yes));
        }

        [Test]
        public void Normalize_WhenConversionEnabled_ProducesUtcAndMapsFlagNo()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = true,
                // Використовуємо локальний таймзон, щоб уникнути проблем з різними ОС
                InputTimeZoneId = TimeZoneInfo.Local.Id
            };
            var service = CreateService(settings);

            var pickupLocal = new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Unspecified);
            var dropoffLocal = new DateTime(2023, 1, 1, 10, 30, 0, DateTimeKind.Unspecified);

            var parsed = new ParsedTripRowDto
            {
                LineNumber = 1,
                PickupLocal = pickupLocal,
                DropoffLocal = dropoffLocal,
                PassengerCount = 1,
                TripDistance = 10.0m,
                StoreAndFwdFlagRaw = "n",
                PULocationId = 1,
                DOLocationId = 2,
                FareAmount = 20.0m,
                TipAmount = 5.0m
            };

            // Act
            var result = service.Normalize(parsed);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Trip, Is.Not.Null);

            var trip = result.Trip!;
            Assert.That(trip.PickupUtc.Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That(trip.DropoffUtc.Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That(trip.StoreAndFwdFlag, Is.EqualTo(StoreAndFwdFlag.No));

            // Немає сенсу жорстко перевіряти offset, бо він залежить від машини,
            // достатньо факту, що час коректно сконвертований у UTC.
        }

        // -------------------------
        // Normalize: invalid store_and_fwd_flag
        // -------------------------

        [Test]
        public void Normalize_InvalidStoreAndFwdFlag_ReturnsFailure()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = false
            };
            var service = CreateService(settings);

            var parsed = new ParsedTripRowDto
            {
                LineNumber = 42,
                PickupLocal = new DateTime(2023, 1, 1, 10, 0, 0),
                DropoffLocal = new DateTime(2023, 1, 1, 10, 5, 0),
                PassengerCount = 1,
                TripDistance = 1.0m,
                StoreAndFwdFlagRaw = "Z", // не N/Y
                PULocationId = 1,
                DOLocationId = 2,
                FareAmount = 10.0m,
                TipAmount = 1.0m
            };

            // Act
            var result = service.Normalize(parsed);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Trip, Is.Null);
            Assert.That(result.ErrorMessage, Is.Not.Null.And.Contains("store_and_fwd_flag"));
        }

        // -------------------------
        // Normalize: domain validation failure
        // -------------------------

        [Test]
        public void Normalize_DropoffBeforePickup_DomainValidationFailsAndReturnsFailure()
        {
            // Arrange
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = false
            };
            var service = CreateService(settings);

            // dropoff < pickup -> Trip ctor має кинути ArgumentException
            var parsed = new ParsedTripRowDto
            {
                LineNumber = 7,
                PickupLocal = new DateTime(2023, 1, 1, 10, 0, 0),
                DropoffLocal = new DateTime(2023, 1, 1, 9, 55, 0),
                PassengerCount = 1,
                TripDistance = 1.0m,
                StoreAndFwdFlagRaw = "N",
                PULocationId = 1,
                DOLocationId = 2,
                FareAmount = 10.0m,
                TipAmount = 2.0m
            };

            // Act
            var result = service.Normalize(parsed);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Trip, Is.Null);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("domain validation").IgnoreCase);
        }

        // -------------------------
        // Normalize: null argument
        // -------------------------

        [Test]
        public void Normalize_NullParsedRow_ThrowsArgumentNullException()
        {
            var settings = new EtlSettingsDto
            {
                EnableTimeZoneConversion = false
            };
            var service = CreateService(settings);

            Assert.Throws<ArgumentNullException>(() => service.Normalize(null!));
        }
    }
}
