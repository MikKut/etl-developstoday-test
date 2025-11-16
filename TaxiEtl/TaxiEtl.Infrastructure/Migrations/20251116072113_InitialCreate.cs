using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TaxiEtl.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "dbo");

            migrationBuilder.CreateTable(
                name: "Trips",
                schema: "dbo",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    tpep_pickup_datetime = table.Column<DateTime>(type: "datetime2(0)", nullable: false),
                    tpep_dropoff_datetime = table.Column<DateTime>(type: "datetime2(0)", nullable: false),
                    passenger_count = table.Column<byte>(type: "tinyint", nullable: false),
                    trip_distance = table.Column<decimal>(type: "decimal(9,3)", nullable: false),
                    store_and_fwd_flag = table.Column<string>(type: "varchar(3)", maxLength: 3, nullable: false),
                    PULocationID = table.Column<int>(type: "int", nullable: false),
                    DOLocationID = table.Column<int>(type: "int", nullable: false),
                    fare_amount = table.Column<decimal>(type: "decimal(10,2)", nullable: false),
                    tip_amount = table.Column<decimal>(type: "decimal(10,2)", nullable: false),
                    TravelTimeSeconds = table.Column<int>(type: "int", nullable: false, computedColumnSql: "DATEDIFF(SECOND, [tpep_pickup_datetime], [tpep_dropoff_datetime])", stored: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Trips", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Trips_PULocation_TipAmount",
                schema: "dbo",
                table: "Trips",
                column: "PULocationID")
                .Annotation("SqlServer:Include", new[] { "tip_amount" });

            migrationBuilder.CreateIndex(
                name: "IX_Trips_TravelTimeSeconds",
                schema: "dbo",
                table: "Trips",
                column: "TravelTimeSeconds");

            migrationBuilder.CreateIndex(
                name: "IX_Trips_TripDistance",
                schema: "dbo",
                table: "Trips",
                column: "trip_distance");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Trips",
                schema: "dbo");
        }
    }
}
