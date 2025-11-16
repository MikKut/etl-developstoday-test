using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Application.DTO;
using TaxiEtl.Application.Services;
using TaxiEtl.Domain.Entities;
using TaxiEtl.Domain.ValueObjects;

namespace TaxiEtl.Tests.Application
{
    [TestFixture]
    public class TripEtlPipelineServiceTests
    {
        private Mock<ICsvTripReaderService> _csvTripReaderMock = null!;
        private Mock<ITripRowParserService> _tripRowParserMock = null!;
        private Mock<ITripRowNormalizerService> _tripRowNormalizerMock = null!;
        private Mock<ITripDuplicateDetectorService> _duplicateDetectorMock = null!;
        private Mock<IDuplicateTripWriter> _duplicateTripWriterMock = null!;
        private Mock<IBulkTripInserterService> _bulkTripInserterMock = null!;
        private Mock<ILogger<TripEtlPipelineService>> _loggerMock = null!;

        private EtlSettingsDto _etlSettings = null!;

        [SetUp]
        public void SetUp()
        {
            _csvTripReaderMock = new Mock<ICsvTripReaderService>(MockBehavior.Strict);
            _tripRowParserMock = new Mock<ITripRowParserService>(MockBehavior.Strict);
            _tripRowNormalizerMock = new Mock<ITripRowNormalizerService>(MockBehavior.Strict);
            _duplicateDetectorMock = new Mock<ITripDuplicateDetectorService>(MockBehavior.Strict);
            _duplicateTripWriterMock = new Mock<IDuplicateTripWriter>(MockBehavior.Strict);
            _bulkTripInserterMock = new Mock<IBulkTripInserterService>(MockBehavior.Strict);
            _loggerMock = new Mock<ILogger<TripEtlPipelineService>>();

            _etlSettings = new EtlSettingsDto
            {
                InputCsvPath = "data/sample-cab-data.csv",
                DuplicatesCsvPath = "data/duplicates.csv",
                BatchSize = 2,
                InputDateTimeFormat = "yyyy-MM-dd HH:mm:ss",
                EnableTimeZoneConversion = true,
                InputTimeZoneId = "Eastern Standard Time"
            };
        }

        private TripEtlPipelineService CreatePipeline()
        {
            var options = Options.Create(_etlSettings);

            return new TripEtlPipelineService(
                _csvTripReaderMock.Object,
                _tripRowParserMock.Object,
                _tripRowNormalizerMock.Object,
                _duplicateDetectorMock.Object,
                _duplicateTripWriterMock.Object,
                _bulkTripInserterMock.Object,
                options,
                _loggerMock.Object);
        }

        #region Tests

        [Test]
        public async Task RunAsync_AllRowsValidAndUnique_InsertsAllRowsAndProducesCorrectStats()
        {
            // Arrange
            var rawRow1 = new CsvTripRawRowDto
            {
                LineNumber = 1,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:10:00"
            };

            var rawRow2 = new CsvTripRawRowDto
            {
                LineNumber = 2,
                TpepPickupDatetime = "2023-01-01 11:00:00",
                TpepDropoffDatetime = "2023-01-01 11:20:00"
            };

            _csvTripReaderMock
                .Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken ct) => CreateAsyncEnumerable(rawRow1, rawRow2));

            _tripRowParserMock
                .Setup(p => p.Parse(It.IsAny<CsvTripRawRowDto>()))
                .Returns((CsvTripRawRowDto raw) => new TripRowParseResultDto
                {
                    IsSuccess = true,
                    ParsedRow = new ParsedTripRowDto
                    {
                        LineNumber = raw.LineNumber,
                        PickupLocal = DateTime.Parse(raw.TpepPickupDatetime),
                        DropoffLocal = DateTime.Parse(raw.TpepDropoffDatetime),
                        PassengerCount = 1,
                        TripDistance = 1.0m,
                        StoreAndFwdFlagRaw = "N",
                        PULocationId = 1,
                        DOLocationId = 2,
                        FareAmount = 10.0m,
                        TipAmount = 2.0m
                    }
                });

            _tripRowNormalizerMock
                .Setup(n => n.Normalize(It.IsAny<ParsedTripRowDto>()))
                .Returns((ParsedTripRowDto parsed) =>
                {
                    var trip = new Trip(
                        pickupUtc: DateTime.SpecifyKind(parsed.PickupLocal, DateTimeKind.Utc),
                        dropoffUtc: DateTime.SpecifyKind(parsed.DropoffLocal, DateTimeKind.Utc),
                        passengerCount: parsed.PassengerCount,
                        tripDistance: parsed.TripDistance,
                        storeAndFwdFlag: StoreAndFwdFlag.No,
                        puLocationId: parsed.PULocationId,
                        doLocationId: parsed.DOLocationId,
                        fareAmount: parsed.FareAmount,
                        tipAmount: parsed.TipAmount);

                    return new TripRowNormalizationResultDto
                    {
                        IsSuccess = true,
                        Trip = trip
                    };
                });

            _duplicateDetectorMock
                .Setup(d => d.TryRegister(It.IsAny<Trip>()))
                .Returns(true);

            _duplicateTripWriterMock
                .Setup(w => w.WriteDuplicateAsync(It.IsAny<CsvTripRawRowDto>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var insertedTripsCount = 0;

            _bulkTripInserterMock
                .Setup(b => b.InsertBatchAsync(
                    It.IsAny<IReadOnlyCollection<Trip>>(),
                    It.IsAny<CancellationToken>()))
                .Callback<IReadOnlyCollection<Trip>, CancellationToken>((batch, ct) =>
                {
                    insertedTripsCount += batch.Count;
                })
                .Returns(Task.CompletedTask);

            var pipeline = CreatePipeline();

            // Act
            var stats = await pipeline.RunAsync();

            // Assert
            Assert.That(stats.TotalRowsRead, Is.EqualTo(2));
            Assert.That(stats.ParsedRows, Is.EqualTo(2));
            Assert.That(stats.InvalidRows, Is.EqualTo(0));
            Assert.That(stats.DuplicateRows, Is.EqualTo(0));
            Assert.That(stats.InsertedRows, Is.EqualTo(2));
            Assert.That(stats.DuplicatesFileRows, Is.EqualTo(0));

            Assert.That(insertedTripsCount, Is.EqualTo(2));

            _bulkTripInserterMock.Verify(
                b => b.InsertBatchAsync(
                    It.IsAny<IReadOnlyCollection<Trip>>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);

            _duplicateTripWriterMock.Verify(
                w => w.WriteDuplicateAsync(It.IsAny<CsvTripRawRowDto>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Test]
        public async Task RunAsync_SecondRowIsDuplicate_WritesDuplicateAndDoesNotInsertIt()
        {
            // Arrange
            var rawRow1 = new CsvTripRawRowDto { LineNumber = 1 };
            var rawRow2 = new CsvTripRawRowDto { LineNumber = 2 }; // duplicate key

            _csvTripReaderMock
                .Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken ct) => CreateAsyncEnumerable(rawRow1, rawRow2));

            _tripRowParserMock
                .Setup(p => p.Parse(It.IsAny<CsvTripRawRowDto>()))
                .Returns((CsvTripRawRowDto raw) => new TripRowParseResultDto
                {
                    IsSuccess = true,
                    ParsedRow = new ParsedTripRowDto
                    {
                        LineNumber = raw.LineNumber,
                        PickupLocal = new DateTime(2023, 1, 1, 10, 0, 0),
                        DropoffLocal = new DateTime(2023, 1, 1, 10, 10, 0),
                        PassengerCount = 1,
                        TripDistance = 1.0m,
                        StoreAndFwdFlagRaw = "N",
                        PULocationId = 1,
                        DOLocationId = 2,
                        FareAmount = 10.0m,
                        TipAmount = 2.0m
                    }
                });

            _tripRowNormalizerMock
                .Setup(n => n.Normalize(It.IsAny<ParsedTripRowDto>()))
                .Returns((ParsedTripRowDto parsed) =>
                {
                    var trip = new Trip(
                        pickupUtc: DateTime.SpecifyKind(parsed.PickupLocal, DateTimeKind.Utc),
                        dropoffUtc: DateTime.SpecifyKind(parsed.DropoffLocal, DateTimeKind.Utc),
                        passengerCount: parsed.PassengerCount,
                        tripDistance: parsed.TripDistance,
                        storeAndFwdFlag: StoreAndFwdFlag.No,
                        puLocationId: parsed.PULocationId,
                        doLocationId: parsed.DOLocationId,
                        fareAmount: parsed.FareAmount,
                        tipAmount: parsed.TipAmount);

                    return new TripRowNormalizationResultDto
                    {
                        IsSuccess = true,
                        Trip = trip
                    };
                });

            _duplicateDetectorMock
                .SetupSequence(d => d.TryRegister(It.IsAny<Trip>()))
                .Returns(true)   // first row -> new
                .Returns(false); // second row -> duplicate

            var duplicatesWritten = 0;

            _duplicateTripWriterMock
                .Setup(w => w.WriteDuplicateAsync(It.IsAny<CsvTripRawRowDto>(), It.IsAny<CancellationToken>()))
                .Callback(() => duplicatesWritten++)
                .Returns(Task.CompletedTask);

            var insertedTripsCount = 0;

            _bulkTripInserterMock
                .Setup(b => b.InsertBatchAsync(
                    It.IsAny<IReadOnlyCollection<Trip>>(),
                    It.IsAny<CancellationToken>()))
                .Callback<IReadOnlyCollection<Trip>, CancellationToken>((batch, ct) =>
                {
                    insertedTripsCount += batch.Count;
                })
                .Returns(Task.CompletedTask);

            var pipeline = CreatePipeline();

            // Act
            var stats = await pipeline.RunAsync();

            // Assert
            Assert.That(stats.TotalRowsRead, Is.EqualTo(2));
            Assert.That(stats.ParsedRows, Is.EqualTo(2));
            Assert.That(stats.InvalidRows, Is.EqualTo(0));
            Assert.That(stats.DuplicateRows, Is.EqualTo(1));
            Assert.That(stats.InsertedRows, Is.EqualTo(1));
            Assert.That(stats.DuplicatesFileRows, Is.EqualTo(1));

            Assert.That(insertedTripsCount, Is.EqualTo(1));
            Assert.That(duplicatesWritten, Is.EqualTo(1));

            _bulkTripInserterMock.Verify(
                b => b.InsertBatchAsync(
                    It.IsAny<IReadOnlyCollection<Trip>>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);

            _duplicateTripWriterMock.Verify(
                w => w.WriteDuplicateAsync(It.IsAny<CsvTripRawRowDto>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Test]
        public async Task RunAsync_ParseFailure_RowIsCountedAsInvalid_AndSkipped()
        {
            // Arrange
            var rawRow1 = new CsvTripRawRowDto { LineNumber = 1 };
            var rawRow2 = new CsvTripRawRowDto { LineNumber = 2 };

            _csvTripReaderMock
                .Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken ct) => CreateAsyncEnumerable(rawRow1, rawRow2));

            _tripRowParserMock
                .Setup(p => p.Parse(It.IsAny<CsvTripRawRowDto>()))
                .Returns((CsvTripRawRowDto raw) =>
                {
                    if (raw.LineNumber == 1)
                    {
                        return new TripRowParseResultDto
                        {
                            IsSuccess = false,
                            ParsedRow = null,
                            ErrorMessage = "Invalid datetime"
                        };
                    }

                    return new TripRowParseResultDto
                    {
                        IsSuccess = true,
                        ParsedRow = new ParsedTripRowDto
                        {
                            LineNumber = raw.LineNumber,
                            PickupLocal = new DateTime(2023, 1, 1, 10, 0, 0),
                            DropoffLocal = new DateTime(2023, 1, 1, 10, 10, 0),
                            PassengerCount = 1,
                            TripDistance = 1.0m,
                            StoreAndFwdFlagRaw = "N",
                            PULocationId = 1,
                            DOLocationId = 2,
                            FareAmount = 10.0m,
                            TipAmount = 2.0m
                        }
                    };
                });

            _tripRowNormalizerMock
                .Setup(n => n.Normalize(It.IsAny<ParsedTripRowDto>()))
                .Returns((ParsedTripRowDto parsed) =>
                {
                    var trip = new Trip(
                        pickupUtc: DateTime.SpecifyKind(parsed.PickupLocal, DateTimeKind.Utc),
                        dropoffUtc: DateTime.SpecifyKind(parsed.DropoffLocal, DateTimeKind.Utc),
                        passengerCount: parsed.PassengerCount,
                        tripDistance: parsed.TripDistance,
                        storeAndFwdFlag: StoreAndFwdFlag.No,
                        puLocationId: parsed.PULocationId,
                        doLocationId: parsed.DOLocationId,
                        fareAmount: parsed.FareAmount,
                        tipAmount: parsed.TipAmount);

                    return new TripRowNormalizationResultDto
                    {
                        IsSuccess = true,
                        Trip = trip
                    };
                });

            _duplicateDetectorMock
                .Setup(d => d.TryRegister(It.IsAny<Trip>()))
                .Returns(true);

            _duplicateTripWriterMock
                .Setup(w => w.WriteDuplicateAsync(It.IsAny<CsvTripRawRowDto>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var insertedTripsCount = 0;

            _bulkTripInserterMock
                .Setup(b => b.InsertBatchAsync(
                    It.IsAny<IReadOnlyCollection<Trip>>(),
                    It.IsAny<CancellationToken>()))
                .Callback<IReadOnlyCollection<Trip>, CancellationToken>((batch, ct) =>
                {
                    insertedTripsCount += batch.Count;
                })
                .Returns(Task.CompletedTask);

            var pipeline = CreatePipeline();

            // Act
            var stats = await pipeline.RunAsync();

            // Assert
            Assert.That(stats.TotalRowsRead, Is.EqualTo(2));
            Assert.That(stats.ParsedRows, Is.EqualTo(1));   // only row 2 parsed successfully
            Assert.That(stats.InvalidRows, Is.EqualTo(1));  // row 1 is invalid
            Assert.That(stats.DuplicateRows, Is.EqualTo(0));
            Assert.That(stats.InsertedRows, Is.EqualTo(1)); // only row 2 inserted
            Assert.That(stats.DuplicatesFileRows, Is.EqualTo(0));

            Assert.That(insertedTripsCount, Is.EqualTo(1));

            // Normalize must be called only once (for the valid row)
            _tripRowNormalizerMock.Verify(
                n => n.Normalize(It.IsAny<ParsedTripRowDto>()),
                Times.Once);

            // Duplicate detector also once
            _duplicateDetectorMock.Verify(
                d => d.TryRegister(It.IsAny<Trip>()),
                Times.Once);
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Helper to create an IAsyncEnumerable from a set of rows.
        /// </summary>
        private static async IAsyncEnumerable<CsvTripRawRowDto> CreateAsyncEnumerable(
            params CsvTripRawRowDto[] rows)
        {
            foreach (var row in rows)
            {
                yield return row;
                // Ensure asynchronous behavior
                await Task.Yield();
            }
        }

        #endregion
    }
}
