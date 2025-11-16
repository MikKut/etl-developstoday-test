using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using TaxiEtl.Application.Constants;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Infrastructure.Persistence.Configurations
{
    public class TripConfiguration : IEntityTypeConfiguration<Trip>
    {
        public void Configure(EntityTypeBuilder<Trip> builder)
        {
            // Table
            builder.ToTable("Trips", "dbo");

            // Primary key
            builder.HasKey(t => t.Id);

            builder.Property(t => t.Id)
                .ValueGeneratedOnAdd();

            // tpep_pickup_datetime (UTC)
            builder.Property(t => t.PickupUtc)
                .HasColumnName(TripFieldNames.PickupDateTime)
                .HasColumnType("datetime2(0)")
                .IsRequired();

            // tpep_dropoff_datetime (UTC)
            builder.Property(t => t.DropoffUtc)
                .HasColumnName(TripFieldNames.DropoffDateTime)
                .HasColumnType("datetime2(0)")
                .IsRequired();

            // passenger_count
            builder.Property(t => t.PassengerCount)
                .HasColumnName(TripFieldNames.PassengerCount)
                .HasColumnType("tinyint")
                .IsRequired();

            // trip_distance
            builder.Property(t => t.TripDistance)
                .HasColumnName(TripFieldNames.TripDistance)
                .HasColumnType("decimal(9,3)")
                .IsRequired();

            // store_and_fwd_flag (enum -> string "No"/"Yes")
            builder.Property(t => t.StoreAndFwdFlag)
                .HasColumnName(TripFieldNames.StoreAndFwdFlag)
                .HasMaxLength(3)
                .HasColumnType("varchar(3)")
                .HasConversion<string>() // Store enum as "No"/"Yes"
                .IsRequired();

            // PULocationID
            builder.Property(t => t.PULocationId)
                .HasColumnName(TripFieldNames.PULocationId)
                .HasColumnType("int")
                .IsRequired();

            // DOLocationID
            builder.Property(t => t.DOLocationId)
                .HasColumnName(TripFieldNames.DOLocationId)
                .HasColumnType("int")
                .IsRequired();

            // fare_amount
            builder.Property(t => t.FareAmount)
                .HasColumnName(TripFieldNames.FareAmount)
                .HasColumnType("decimal(10,2)")
                .IsRequired();

            // tip_amount
            builder.Property(t => t.TipAmount)
                .HasColumnName(TripFieldNames.TipAmount)
                .HasColumnType("decimal(10,2)")
                .IsRequired();

            builder.Property(t => t.TravelTimeSeconds)
                .HasColumnName(TripFieldNames.TravelTimeSeconds)
                .HasColumnType("int")
                .HasComputedColumnSql(
                    $"DATEDIFF(SECOND, [{TripFieldNames.PickupDateTime}], [{TripFieldNames.DropoffDateTime}])",
                    stored: true);

            // Index for: "which PULocationId has the highest average tip_amount"
            builder.HasIndex(t => t.PULocationId)
                .HasDatabaseName("IX_Trips_PULocation_TipAmount")
                .IncludeProperties(t => t.TipAmount);

            // Index for: "top 100 longest fares by trip_distance"
            builder.HasIndex(t => t.TripDistance)
                .HasDatabaseName("IX_Trips_TripDistance");

            // Index for: "top 100 longest fares by time spent traveling"
            builder.HasIndex("TravelTimeSeconds")
                .HasDatabaseName("IX_Trips_TravelTimeSeconds");
        }
    }
}
