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
    public class CsvTripReaderServiceTests
    {
        private string _tempDir = null!;
        private Mock<ILogger<CsvTripReaderService>> _loggerMock = null!;

        [SetUp]
        public void SetUp()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "TaxiEtlTests_Reader_" + Guid.NewGuid());
            Directory.CreateDirectory(_tempDir);

            _loggerMock = new Mock<ILogger<CsvTripReaderService>>();
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
                // Не валимо тести, якщо при прибиранні щось пішло не так
            }
        }

        private CsvTripReaderService CreateReader(string csvPath, string? delimiter = null)
        {
            var settings = new EtlSettingsDto
            {
                InputCsvPath = csvPath,
                CsvDelimiter = delimiter
            };

            var options = Options.Create(settings);
            return new CsvTripReaderService(options, _loggerMock.Object);
        }

        private string CreateCsvFile(string fileName, IEnumerable<string> lines)
        {
            var path = Path.Combine(_tempDir, fileName);
            File.WriteAllLines(path, lines);
            return path;
        }

        // -------------------------
        // Ctor
        // -------------------------

        [Test]
        public void Ctor_NullOptions_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(
                () => new CsvTripReaderService(null!, _loggerMock.Object));
        }

        [Test]
        public void Ctor_NullLogger_ThrowsArgumentNullException()
        {
            var options = Options.Create(new EtlSettingsDto
            {
                InputCsvPath = "dummy.csv"
            });

            Assert.Throws<ArgumentNullException>(
                () => new CsvTripReaderService(options, null!));
        }

        [Test]
        public void Ctor_EmptyInputCsvPath_ThrowsArgumentException()
        {
            var options = Options.Create(new EtlSettingsDto
            {
                InputCsvPath = "   "
            });

            var ex = Assert.Throws<ArgumentException>(
                () => new CsvTripReaderService(options, _loggerMock.Object));

            StringAssert.Contains("InputCsvPath", ex!.Message);
        }

        // -------------------------
        // ReadAsync: basic happy path
        // -------------------------

        [Test]
        public async Task ReadAsync_ValidCsv_ReadsRowsAndMapsColumnsByName()
        {
            // Arrange
            var lines = new[]
            {
                // header (можна в будь-якому порядку, важливі імена колонок)
                string.Join(",",
                    TripFieldNames.PULocationId,
                    TripFieldNames.PickupDateTime,
                    TripFieldNames.DropoffDateTime,
                    TripFieldNames.PassengerCount,
                    TripFieldNames.TripDistance,
                    TripFieldNames.StoreAndFwdFlag,
                    TripFieldNames.DOLocationId,
                    TripFieldNames.FareAmount,
                    TripFieldNames.TipAmount),

                // row 1
                "10,2023-01-01 10:00:00,2023-01-01 10:05:00,1,1.23,N,20,10.00,2.50",
                // row 2
                "11,2023-01-01 11:00:00,2023-01-01 11:10:00,2,3.50,Y,21,20.00,5.00"
            };

            var path = CreateCsvFile("trips.csv", lines);

            // Використаємо абсолютний шлях, щоб не залежати від BaseDirectory
            var reader = CreateReader(path);

            var resultRows = new List<CsvTripRawRowDto>();

            // Act
            await foreach (var row in reader.ReadAsync(CancellationToken.None))
            {
                resultRows.Add(row);
            }

            // Assert
            Assert.That(resultRows.Count, Is.EqualTo(2));

            var r1 = resultRows[0];
            Assert.That(r1.LineNumber, Is.EqualTo(1));
            Assert.That(r1.PULocationId, Is.EqualTo("10"));
            Assert.That(r1.TpepPickupDatetime, Is.EqualTo("2023-01-01 10:00:00"));
            Assert.That(r1.TpepDropoffDatetime, Is.EqualTo("2023-01-01 10:05:00"));
            Assert.That(r1.PassengerCount, Is.EqualTo("1"));
            Assert.That(r1.TripDistance, Is.EqualTo("1.23"));
            Assert.That(r1.StoreAndFwdFlag, Is.EqualTo("N"));
            Assert.That(r1.DOLocationId, Is.EqualTo("20"));
            Assert.That(r1.FareAmount, Is.EqualTo("10.00"));
            Assert.That(r1.TipAmount, Is.EqualTo("2.50"));

            var r2 = resultRows[1];
            Assert.That(r2.LineNumber, Is.EqualTo(2));
            Assert.That(r2.PULocationId, Is.EqualTo("11"));
            Assert.That(r2.TpepPickupDatetime, Is.EqualTo("2023-01-01 11:00:00"));
            Assert.That(r2.PassengerCount, Is.EqualTo("2"));
            Assert.That(r2.StoreAndFwdFlag, Is.EqualTo("Y"));
        }

        // -------------------------
        // ReadAsync: missing file
        // -------------------------

        [Test]
        public void ReadAsync_WhenFileDoesNotExist_ThrowsFileNotFoundException()
        {
            // Arrange
            var nonExistingPath = Path.Combine(_tempDir, "no_such_file.csv");
            var reader = CreateReader(nonExistingPath);

            // Act + Assert
            Assert.ThrowsAsync<FileNotFoundException>(async () =>
            {
                await foreach (var _ in reader.ReadAsync(CancellationToken.None))
                {
                    // never reached
                }
            });
        }

        // -------------------------
        // ReadAsync: empty file / missing header
        // -------------------------

        [Test]
        public void ReadAsync_EmptyFile_ThrowsInvalidDataException()
        {
            // Arrange
            var path = CreateCsvFile("empty.csv", Array.Empty<string>());
            var reader = CreateReader(path);

            // Act + Assert
            Assert.ThrowsAsync<InvalidDataException>(async () =>
            {
                await foreach (var _ in reader.ReadAsync(CancellationToken.None))
                {
                    // never reached
                }
            });
        }

        // -------------------------
        // ReadAsync: missing required column
        // -------------------------

        [Test]
        public void ReadAsync_MissingRequiredColumn_ThrowsInvalidDataException()
        {
            // header без tip_amount
            var lines = new[]
            {
                string.Join(",",
                    TripFieldNames.PickupDateTime,
                    TripFieldNames.DropoffDateTime,
                    TripFieldNames.PassengerCount,
                    TripFieldNames.TripDistance,
                    TripFieldNames.StoreAndFwdFlag,
                    TripFieldNames.PULocationId,
                    TripFieldNames.DOLocationId,
                    TripFieldNames.FareAmount),
                "2023-01-01 10:00:00,2023-01-01 10:05:00,1,1.23,N,10,20,10.00"
            };

            var path = CreateCsvFile("missing_column.csv", lines);
            var reader = CreateReader(path);

            Assert.ThrowsAsync<InvalidDataException>(async () =>
            {
                await foreach (var _ in reader.ReadAsync(CancellationToken.None))
                {
                    // never reached
                }
            });
        }

        // -------------------------
        // ReadAsync: custom delimiter
        // -------------------------

        [Test]
        public async Task ReadAsync_WithCustomDelimiter_SplitsByThatDelimiter()
        {
            // Arrange: використовуємо ; як роздільник
            var lines = new[]
            {
                string.Join(";",
                    TripFieldNames.PickupDateTime,
                    TripFieldNames.DropoffDateTime,
                    TripFieldNames.PassengerCount,
                    TripFieldNames.TripDistance,
                    TripFieldNames.StoreAndFwdFlag,
                    TripFieldNames.PULocationId,
                    TripFieldNames.DOLocationId,
                    TripFieldNames.FareAmount,
                    TripFieldNames.TipAmount),
                "2023-01-01 10:00:00;2023-01-01 10:05:00;1;1.23;N;10;20;10.00;2.50"
            };

            var path = CreateCsvFile("semicolon.csv", lines);

            var settingsDelimiter = ";";
            var reader = CreateReader(path, delimiter: settingsDelimiter);

            var rows = new List<CsvTripRawRowDto>();

            // Act
            await foreach (var row in reader.ReadAsync(CancellationToken.None))
            {
                rows.Add(row);
            }

            // Assert
            Assert.That(rows.Count, Is.EqualTo(1));
            var r = rows[0];

            Assert.That(r.TpepPickupDatetime, Is.EqualTo("2023-01-01 10:00:00"));
            Assert.That(r.TripDistance, Is.EqualTo("1.23"));
            Assert.That(r.StoreAndFwdFlag, Is.EqualTo("N"));
            Assert.That(r.TipAmount, Is.EqualTo("2.50"));
        }

        // -------------------------
        // ReadAsync: blank lines are skipped and not counted
        // -------------------------

        [Test]
        public async Task ReadAsync_BlankLines_AreSkipped_AndLineNumbersCountDataRowsOnly()
        {
            // Arrange
            var lines = new[]
            {
                string.Join(",",
                    TripFieldNames.PickupDateTime,
                    TripFieldNames.DropoffDateTime,
                    TripFieldNames.PassengerCount,
                    TripFieldNames.TripDistance,
                    TripFieldNames.StoreAndFwdFlag,
                    TripFieldNames.PULocationId,
                    TripFieldNames.DOLocationId,
                    TripFieldNames.FareAmount,
                    TripFieldNames.TipAmount),

                // data row 1
                "2023-01-01 10:00:00,2023-01-01 10:05:00,1,1.23,N,10,20,10.00,2.50",
                "   ",  // blank line -> має бути пропущений
                // data row 2
                "2023-01-01 11:00:00,2023-01-01 11:05:00,2,3.21,Y,11,21,20.00,4.00"
            };

            var path = CreateCsvFile("with_blank_lines.csv", lines);
            var reader = CreateReader(path);

            var rows = new List<CsvTripRawRowDto>();

            // Act
            await foreach (var row in reader.ReadAsync(CancellationToken.None))
            {
                rows.Add(row);
            }

            // Assert
            Assert.That(rows.Count, Is.EqualTo(2));
            Assert.That(rows[0].LineNumber, Is.EqualTo(1));
            Assert.That(rows[1].LineNumber, Is.EqualTo(2));
        }
    }
}
