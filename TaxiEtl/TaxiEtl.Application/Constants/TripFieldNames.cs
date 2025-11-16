using System;
using System.Collections.Generic;
using System.Text;

namespace TaxiEtl.Application.Constants
{
    public static class TripFieldNames
    {
        public const string PickupDateTime = "tpep_pickup_datetime";
        public const string DropoffDateTime = "tpep_dropoff_datetime";
        public const string PassengerCount = "passenger_count";
        public const string TripDistance = "trip_distance";
        public const string StoreAndFwdFlag = "store_and_fwd_flag";
        public const string PULocationId = "PULocationID";
        public const string DOLocationId = "DOLocationID";
        public const string FareAmount = "fare_amount";
        public const string TipAmount = "tip_amount";
        public const string TravelTimeSeconds = "TravelTimeSeconds";
    }

}
