namespace TaxiEtl.Infrastructure.Persistence.Schema
{
    public static class TripsTable
    {
        public const string Schema = "[dbo]";
        public const string sName = "[Trips]";
        public static string GetFullName() => string.Concat(Schema, '.', sName);
    }
}
