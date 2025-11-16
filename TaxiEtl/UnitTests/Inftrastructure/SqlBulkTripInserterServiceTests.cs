using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework.Legacy;
using System.Data;
using TaxiEtl.Application.Constants;
using TaxiEtl.Domain.Entities;
using TaxiEtl.Domain.ValueObjects;
using TaxiEtl.Infrastructure.Persistence.Options;
using TaxiEtl.Infrastructure.Persistence.Services;

namespace TaxiEtl.Tests.Infrastructure
{
    [TestFixture]
    public class SqlBulkTripInserterServiceTests
    {
        private Mock<ILogger<SqlBulkTripInserterService>> _loggerMock = null!;

        [SetUp]
        public void SetUp()
        {
            _loggerMock = new Mock<ILogger<SqlBulkTripInserterService>>();
        }

        private SqlBulkTripInserterService CreateInserter(string? connectionString = null)
        {
            var options = Options.Create(new DatabaseOptions
            {
                ConnectionString = connectionString ??
                                   "Server=(localdb)\\MSSQLLocalDB;Database=TaxiEtlTest;Integrated Security=true;TrustServerCertificate=true"
            });

            return new SqlBulkTripInserterService(options, _loggerMock.Object);
        }

        private static Trip CreateSampleTrip(
            DateTime? pickupUtc = null,
            DateTime? dropoffUtc = null,
            byte passengerCount = 1,
            decimal tripDistance = 1.23m,
            StoreAndFwdFlag flag = StoreAndFwdFlag.No,
            int puId = 10,
            int doId = 20,
            decimal fare = 10.00m,
            decimal tip = 2.50m)
        {
            return new Trip(
                pickupUtc ?? new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc),
                dropoffUtc ?? new DateTime(2023, 1, 1, 10, 5, 0, DateTimeKind.Utc),
                passengerCount,
                tripDistance,
                flag,
                puId,
                doId,
                fare,
                tip);
        }

        // -----------------------------
        // Ctor tests
        // -----------------------------

        [Test]
        public void Ctor_NullOptions_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(
                () => new SqlBulkTripInserterService(null!, _loggerMock.Object));
        }

        [Test]
        public void Ctor_NullLogger_ThrowsArgumentNullException()
        {
            var options = Options.Create(new DatabaseOptions
            {
                ConnectionString = "Server=.;Database=TaxiEtl;Trusted_Connection=True;"
            });

            Assert.Throws<ArgumentNullException>(
                () => new SqlBulkTripInserterService(options, null!));
        }

        [Test]
        public void Ctor_EmptyConnectionString_ThrowsArgumentException()
        {
            var options = Options.Create(new DatabaseOptions
            {
                ConnectionString = "   "
            });

            var ex = Assert.Throws<ArgumentException>(
                () => new SqlBulkTripInserterService(options, _loggerMock.Object));

            StringAssert.Contains("Database connection string must be provided", ex!.Message);
        }

        // -----------------------------
        // InsertBatchAsync guards
        // -----------------------------

        [Test]
        public void InsertBatchAsync_NullTrips_ThrowsArgumentNullException()
        {
            var inserter = CreateInserter();

            Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await inserter.InsertBatchAsync(null!, default);
            });
        }

        [Test]
        public async Task InsertBatchAsync_EmptyCollection_DoesNothingAndDoesNotThrow()
        {
            var inserter = CreateInserter();
            var empty = Array.Empty<Trip>();

            // Act + Assert: просто не повинно впасти
            await inserter.InsertBatchAsync(empty, default);
        }

        // -----------------------------
        // BulkSchema.BuildDataTable
        // -----------------------------

        [Test]
        public void BulkSchema_BuildDataTable_CreatesCorrectSchemaAndRows()
        {
            // Arrange
            var trips = new List<Trip>
            {
                CreateSampleTrip(
                    pickupUtc: new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc),
                    dropoffUtc: new DateTime(2023, 1, 1, 10, 5, 0, DateTimeKind.Utc),
                    passengerCount: 1,
                    tripDistance: 1.23m,
                    flag: StoreAndFwdFlag.No,
                    puId: 100,
                    doId: 200,
                    fare: 10.00m,
                    tip: 2.50m),

                CreateSampleTrip(
                    pickupUtc: new DateTime(2023, 1, 1, 11, 0, 0, DateTimeKind.Utc),
                    dropoffUtc: new DateTime(2023, 1, 1, 11, 30, 0, DateTimeKind.Utc),
                    passengerCount: 2,
                    tripDistance: 5.50m,
                    flag: StoreAndFwdFlag.Yes,
                    puId: 101,
                    doId: 201,
                    fare: 25.00m,
                    tip: 6.00m)
            };

            // Act
            DataTable table = SqlBulkTripInserterService.BulkSchema.BuildDataTable(trips);

            // Assert: схема
            Assert.That(table.Columns.Count, Is.EqualTo(9));

            Assert.That(table.Columns[0].ColumnName, Is.EqualTo(TripFieldNames.PickupDateTime));
            Assert.That(table.Columns[0].DataType, Is.EqualTo(typeof(DateTime)));

            Assert.That(table.Columns[1].ColumnName, Is.EqualTo(TripFieldNames.DropoffDateTime));
            Assert.That(table.Columns[1].DataType, Is.EqualTo(typeof(DateTime)));

            Assert.That(table.Columns[2].ColumnName, Is.EqualTo(TripFieldNames.PassengerCount));
            Assert.That(table.Columns[2].DataType, Is.EqualTo(typeof(byte)));

            Assert.That(table.Columns[3].ColumnName, Is.EqualTo(TripFieldNames.TripDistance));
            Assert.That(table.Columns[3].DataType, Is.EqualTo(typeof(decimal)));

            Assert.That(table.Columns[4].ColumnName, Is.EqualTo(TripFieldNames.StoreAndFwdFlag));
            Assert.That(table.Columns[4].DataType, Is.EqualTo(typeof(string)));

            Assert.That(table.Columns[5].ColumnName, Is.EqualTo(TripFieldNames.PULocationId));
            Assert.That(table.Columns[5].DataType, Is.EqualTo(typeof(int)));

            Assert.That(table.Columns[6].ColumnName, Is.EqualTo(TripFieldNames.DOLocationId));
            Assert.That(table.Columns[6].DataType, Is.EqualTo(typeof(int)));

            Assert.That(table.Columns[7].ColumnName, Is.EqualTo(TripFieldNames.FareAmount));
            Assert.That(table.Columns[7].DataType, Is.EqualTo(typeof(decimal)));

            Assert.That(table.Columns[8].ColumnName, Is.EqualTo(TripFieldNames.TipAmount));
            Assert.That(table.Columns[8].DataType, Is.EqualTo(typeof(decimal)));

            // Assert: значення рядків
            Assert.That(table.Rows.Count, Is.EqualTo(2));

            var r1 = table.Rows[0];
            Assert.That((DateTime)r1[TripFieldNames.PickupDateTime],
                Is.EqualTo(new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc)));
            Assert.That((DateTime)r1[TripFieldNames.DropoffDateTime],
                Is.EqualTo(new DateTime(2023, 1, 1, 10, 5, 0, DateTimeKind.Utc)));
            Assert.That((byte)r1[TripFieldNames.PassengerCount], Is.EqualTo((byte)1));
            Assert.That((decimal)r1[TripFieldNames.TripDistance], Is.EqualTo(1.23m));
            Assert.That((string)r1[TripFieldNames.StoreAndFwdFlag], Is.EqualTo("No"));
            Assert.That((int)r1[TripFieldNames.PULocationId], Is.EqualTo(100));
            Assert.That((int)r1[TripFieldNames.DOLocationId], Is.EqualTo(200));
            Assert.That((decimal)r1[TripFieldNames.FareAmount], Is.EqualTo(10.00m));
            Assert.That((decimal)r1[TripFieldNames.TipAmount], Is.EqualTo(2.50m));

            var r2 = table.Rows[1];
            Assert.That((byte)r2[TripFieldNames.PassengerCount], Is.EqualTo((byte)2));
            Assert.That((decimal)r2[TripFieldNames.TripDistance], Is.EqualTo(5.50m));
            Assert.That((string)r2[TripFieldNames.StoreAndFwdFlag], Is.EqualTo("Yes"));
            Assert.That((int)r2[TripFieldNames.PULocationId], Is.EqualTo(101));
            Assert.That((int)r2[TripFieldNames.DOLocationId], Is.EqualTo(201));
            Assert.That((decimal)r2[TripFieldNames.FareAmount], Is.EqualTo(25.00m));
            Assert.That((decimal)r2[TripFieldNames.TipAmount], Is.EqualTo(6.00m));
        }

        // -----------------------------
        // BulkSchema.AddColumnMappings
        // -----------------------------

        [Test]
        public void BulkSchema_AddColumnMappings_AddsMappingsForAllColumns()
        {
            // Arrange
            // Створюємо "порожній" SqlBulkCopy з фейковим конекшеном, але НЕ викликаємо WriteToServer.
            using var connection = new SqlConnection("Server=.;Database=master;Trusted_Connection=True;");
            using var bulkCopy = new SqlBulkCopy(connection);

            // Act
            SqlBulkTripInserterService.BulkSchema.AddColumnMappings(bulkCopy);

            // Assert
            Assert.That(bulkCopy.ColumnMappings.Count, Is.EqualTo(9));

            // Перевіримо, що кожна колонка мапиться сама на себе
            var columnNames = new[]
            {
                TripFieldNames.PickupDateTime,
                TripFieldNames.DropoffDateTime,
                TripFieldNames.PassengerCount,
                TripFieldNames.TripDistance,
                TripFieldNames.StoreAndFwdFlag,
                TripFieldNames.PULocationId,
                TripFieldNames.DOLocationId,
                TripFieldNames.FareAmount,
                TripFieldNames.TipAmount
            };

            foreach (var name in columnNames)
            {
                bool found = false;
                foreach (SqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
                {
                    if (string.Equals(mapping.SourceColumn, name, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(mapping.DestinationColumn, name, StringComparison.OrdinalIgnoreCase))
                    {
                        found = true;
                        break;
                    }
                }

                Assert.That(found, Is.True, $"Column mapping for '{name}' should exist.");
            }
        }
    }
}
