using System;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using TaxiEtl.Application.DTO;
using TaxiEtl.Application.Services;

namespace TaxiEtl.Tests.Application
{
    [TestFixture]
    public class TripRowParserServiceTests
    {
        private static TripRowParserService CreateParser(string? format = "yyyy-MM-dd HH:mm:ss")
        {
            var settings = new EtlSettingsDto
            {
                InputDateTimeFormat = format
            };

            var options = Options.Create(settings);
            return new TripRowParserService(options);
        }

        // -------------------------
        // Ctor
        // -------------------------

        [Test]
        public void Ctor_NullOptions_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(
                () => new TripRowParserService(null!));
        }

        // -------------------------
        // Happy path
        // -------------------------

        [Test]
        public void Parse_ValidRow_WithExactFormat_ReturnsSuccessAndParsedValues()
        {
            // Arrange
            var parser = CreateParser("yyyy-MM-dd HH:mm:ss");

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 1,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:15:30",
                PassengerCount = " 2 ",
                TripDistance = " 3.75 ",
                StoreAndFwdFlag = " y ",
                PULocationId = "132",
                DOLocationId = "256",
                FareAmount = " 12.50 ",
                TipAmount = " 3.00 "
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.ErrorMessage, Is.Null.Or.Empty);
            Assert.That(result.ParsedRow, Is.Not.Null);

            var parsed = result.ParsedRow!;

            Assert.That(parsed.LineNumber, Is.EqualTo(1));
            Assert.That(parsed.PickupLocal, Is.EqualTo(new DateTime(2023, 1, 1, 10, 0, 0)));
            Assert.That(parsed.DropoffLocal, Is.EqualTo(new DateTime(2023, 1, 1, 10, 15, 30)));

            Assert.That(parsed.PassengerCount, Is.EqualTo((byte)2));
            Assert.That(parsed.TripDistance, Is.EqualTo(3.75m));
            Assert.That(parsed.PULocationId, Is.EqualTo(132));
            Assert.That(parsed.DOLocationId, Is.EqualTo(256));
            Assert.That(parsed.FareAmount, Is.EqualTo(12.50m));
            Assert.That(parsed.TipAmount, Is.EqualTo(3.00m));

            // важливо: прапорець нормалізується до trimmed + upper case
            Assert.That(parsed.StoreAndFwdFlagRaw, Is.EqualTo("Y"));
        }

        [Test]
        public void Parse_ValidRow_WithoutFormat_UsesDefaultParsing()
        {
            // Arrange: відсутність InputDateTimeFormat → гілка DateTime.TryParse
            var parser = CreateParser(format: null);

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 2,
                TpepPickupDatetime = "2023-01-01T10:00:00",
                TpepDropoffDatetime = "2023-01-01T10:10:00",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.ParsedRow, Is.Not.Null);

            var parsed = result.ParsedRow!;
            Assert.That(parsed.PickupLocal, Is.EqualTo(DateTime.Parse("2023-01-01T10:00:00")));
            Assert.That(parsed.DropoffLocal, Is.EqualTo(DateTime.Parse("2023-01-01T10:10:00")));
        }

        // -------------------------
        // Errors: datetime
        // -------------------------

        [Test]
        public void Parse_InvalidPickupDate_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser("yyyy-MM-dd HH:mm:ss");

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 3,
                TpepPickupDatetime = "not-a-date",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ParsedRow, Is.Null);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("tpep_pickup_datetime"));
        }

        [Test]
        public void Parse_EmptyDropoffDate_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser("yyyy-MM-dd HH:mm:ss");

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 4,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "   ",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("tpep_dropoff_datetime"));
        }

        // -------------------------
        // Errors: numeric fields
        // -------------------------

        [Test]
        public void Parse_InvalidPassengerCount_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser();

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 5,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "abc", // not a number
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("passenger_count"));
        }

        [Test]
        public void Parse_NegativeTripDistance_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser();

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 6,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "1",
                TripDistance = "-1.0", // < 0
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("trip_distance"));
        }

        [Test]
        public void Parse_InvalidFareAmount_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser();

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 7,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "xx", // invalid decimal
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("fare_amount"));
        }

        [Test]
        public void Parse_InvalidTipAmount_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser();

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 8,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "N",
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "" // empty
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("tip_amount"));
        }

        // -------------------------
        // Errors: string field
        // -------------------------

        [Test]
        public void Parse_EmptyStoreAndFwdFlag_ReturnsFailure()
        {
            // Arrange
            var parser = CreateParser();

            var raw = new CsvTripRawRowDto
            {
                LineNumber = 9,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00",
                PassengerCount = "1",
                TripDistance = "1.0",
                StoreAndFwdFlag = "   ", // empty/whitespace
                PULocationId = "1",
                DOLocationId = "2",
                FareAmount = "5.00",
                TipAmount = "1.00"
            };

            // Act
            var result = parser.Parse(raw);

            // Assert
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.ErrorMessage, Is.Not.Null);
            Assert.That(result.ErrorMessage!, Does.Contain("store_and_fwd_flag"));
        }

        // -------------------------
        // Null argument
        // -------------------------

        [Test]
        public void Parse_NullRawRow_ThrowsArgumentNullException()
        {
            var parser = CreateParser();
            Assert.Throws<ArgumentNullException>(() => parser.Parse(null!));
        }
    }
}
