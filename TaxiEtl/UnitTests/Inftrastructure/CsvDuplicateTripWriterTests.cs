using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework.Legacy;
using TaxiEtl.Application.Constants;
using TaxiEtl.Application.DTO;
using TaxiEtl.Infrastructure.Persistence.Services;

namespace TaxiEtl.Tests.Infrastructure
{
    [TestFixture]
    public class CsvDuplicateTripWriterTests
    {
        private string _tempDir = null!;
        private Mock<ILogger<CsvDuplicateTripWriter>> _loggerMock = null!;

        [SetUp]
        public void SetUp()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "TaxiEtlTests_Duplicates_" + Guid.NewGuid());
            Directory.CreateDirectory(_tempDir);

            _loggerMock = new Mock<ILogger<CsvDuplicateTripWriter>>();
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                if (Directory.Exists(_tempDir))
                {
                    Directory.Delete(_tempDir, recursive: true);
                }
            }
            catch
            {
                // If cleanup fails, we don't want tests to fail because of that.
            }
        }

        private CsvDuplicateTripWriter CreateWriter(string duplicatesPath)
        {
            var settings = new EtlSettingsDto
            {
                DuplicatesCsvPath = duplicatesPath
            };

            var options = Options.Create(settings);
            return new CsvDuplicateTripWriter(options, _loggerMock.Object);
        }

        private static CsvTripRawRowDto CreateSampleRawRow(int lineNumber = 1)
        {
            return new CsvTripRawRowDto
            {
                LineNumber = lineNumber,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:05:00",
                PassengerCount = "1",
                TripDistance = "1.23",
                StoreAndFwdFlag = "N",
                PULocationId = "10",
                DOLocationId = "20",
                FareAmount = "10.00",
                TipAmount = "2.50"
            };
        }

        // -------------------------
        // Ctor
        // -------------------------

        [Test]
        public void Ctor_NullOptions_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(
                () => new CsvDuplicateTripWriter(null!, _loggerMock.Object));
        }

        [Test]
        public void Ctor_NullLogger_ThrowsArgumentNullException()
        {
            var options = Options.Create(new EtlSettingsDto
            {
                DuplicatesCsvPath = "some-path.csv"
            });

            Assert.Throws<ArgumentNullException>(
                () => new CsvDuplicateTripWriter(options, null!));
        }

        [Test]
        public void Ctor_EmptyDuplicatesPath_ThrowsArgumentException()
        {
            var options = Options.Create(new EtlSettingsDto
            {
                DuplicatesCsvPath = "   "
            });

            var ex = Assert.Throws<ArgumentException>(
                () => new CsvDuplicateTripWriter(options, _loggerMock.Object));

            StringAssert.Contains("DuplicatesCsvPath", ex!.Message);
        }

        // -------------------------
        // WriteDuplicateAsync: basic behavior
        // -------------------------

        [Test]
        public async Task WriteDuplicateAsync_FirstCall_CreatesFileWithHeaderAndOneRow()
        {
            // Arrange
            var path = Path.Combine(_tempDir, "duplicates.csv");
            var writer = CreateWriter(path);
            var rawRow = CreateSampleRawRow(lineNumber: 1);

            // Act
            await writer.WriteDuplicateAsync(rawRow, CancellationToken.None);

            // Assert
            Assert.That(File.Exists(path), Is.True, "duplicates.csv should be created.");

            var lines = await File.ReadAllLinesAsync(path);
            Assert.That(lines.Length, Is.EqualTo(2), "Header + 1 data row expected.");

            // Header
            var expectedHeader = string.Join(",",
                "LineNumber",
                TripFieldNames.PickupDateTime,
                TripFieldNames.DropoffDateTime,
                TripFieldNames.PassengerCount,
                TripFieldNames.TripDistance,
                TripFieldNames.StoreAndFwdFlag,
                TripFieldNames.PULocationId,
                TripFieldNames.DOLocationId,
                TripFieldNames.FareAmount,
                TripFieldNames.TipAmount);

            Assert.That(lines[0], Is.EqualTo(expectedHeader));

            // Data row
            var expectedData = "1,2023-01-01 10:00:00,2023-01-01 10:05:00,1,1.23,N,10,20,10.00,2.50";
            Assert.That(lines[1], Is.EqualTo(expectedData));
        }

        [Test]
        public async Task WriteDuplicateAsync_SecondCall_AppendsRowWithoutDuplicatingHeader()
        {
            // Arrange
            var path = Path.Combine(_tempDir, "duplicates.csv");
            var writer = CreateWriter(path);

            var rawRow1 = CreateSampleRawRow(lineNumber: 1);
            var rawRow2 = CreateSampleRawRow(lineNumber: 2);

            // Act
            await writer.WriteDuplicateAsync(rawRow1, CancellationToken.None);
            await writer.WriteDuplicateAsync(rawRow2, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(path);

            Assert.That(lines.Length, Is.EqualTo(3), "Header + 2 data rows expected.");

            // First line is header, should not be duplicated.
            Assert.That(lines[0].StartsWith("LineNumber,"), Is.True);

            // Data rows should correspond to line numbers 1 and 2.
            Assert.That(lines[1].StartsWith("1,"), Is.True);
            Assert.That(lines[2].StartsWith("2,"), Is.True);
        }

        // -------------------------
        // WriteDuplicateAsync: directory creation
        // -------------------------

        [Test]
        public async Task WriteDuplicateAsync_WhenDirectoryDoesNotExist_CreatesDirectory()
        {
            // Arrange
            var innerDir = Path.Combine(_tempDir, "nested", "inner");
            var path = Path.Combine(innerDir, "duplicates.csv");

            // Ensure directory doesn't exist yet
            if (Directory.Exists(innerDir))
            {
                Directory.Delete(innerDir, recursive: true);
            }

            var writer = CreateWriter(path);
            var rawRow = CreateSampleRawRow(lineNumber: 1);

            // Act
            await writer.WriteDuplicateAsync(rawRow, CancellationToken.None);

            // Assert
            Assert.That(Directory.Exists(innerDir), Is.True, "Writer should create directory if missing.");
            Assert.That(File.Exists(path), Is.True);
        }

        // -------------------------
        // WriteDuplicateAsync: CSV escaping
        // -------------------------

        [Test]
        public async Task WriteDuplicateAsync_FieldWithComma_IsQuotedInCsv()
        {
            // Arrange
            var path = Path.Combine(_tempDir, "duplicates_with_commas.csv");
            var writer = CreateWriter(path);

            var rawRow = new CsvTripRawRowDto
            {
                LineNumber = 1,
                TpepPickupDatetime = "2023-01-01 10:00:00",
                TpepDropoffDatetime = "2023-01-01 10:05:00",
                PassengerCount = "1",
                TripDistance = "1.23",
                StoreAndFwdFlag = "N",
                PULocationId = "10",
                // Put a comma here to force quoting
                DOLocationId = "20,21",
                FareAmount = "10.00",
                TipAmount = "2.50"
            };

            // Act
            await writer.WriteDuplicateAsync(rawRow, CancellationToken.None);

            // Assert
            var lines = await File.ReadAllLinesAsync(path);
            Assert.That(lines.Length, Is.EqualTo(2));

            var dataLine = lines[1];
            // Expect DOLocationId field to be quoted as "20,21"
            Assert.That(dataLine, Does.Contain("\"20,21\""));
        }
    }
}
