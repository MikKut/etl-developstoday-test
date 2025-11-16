using Microsoft.EntityFrameworkCore;
using TaxiEtl.Domain.Entities;

namespace TaxiEtl.Infrastructure.Persistence
{

    public class TaxiEtlDbContext : DbContext
    {
        public TaxiEtlDbContext(DbContextOptions<TaxiEtlDbContext> options)
            : base(options)
        {
        }

        /// <summary>
        /// Main trips dataset mapped to dbo.Trips table.
        /// </summary>
        public DbSet<Trip> Trips => Set<Trip>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("dbo");
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TaxiEtlDbContext).Assembly);
            base.OnModelCreating(modelBuilder);
        }
    }
}