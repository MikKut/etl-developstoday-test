using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TaxiEtl.Application;
using TaxiEtl.Application.Abstractions;
using TaxiEtl.Infrastructure;

static string FindConfigRoot()
{
    // Папка, де лежить твоя DLL/EXE, НЕ робоча директорія процесу
    var dir = AppContext.BaseDirectory;

    while (dir != null)
    {
        var candidate = Path.Combine(dir, "appsettings.json");
        if (File.Exists(candidate))
            return dir;

        dir = Directory.GetParent(dir)?.FullName;
    }

    throw new FileNotFoundException("Не знайшов appsettings.json ні в одній з батьківських папок");
}

var configRoot = FindConfigRoot();

var host = Host.CreateDefaultBuilder(args)
    // кажемо хосту: корінь контенту = там, де лежить appsettings.json
    .UseContentRoot(configRoot)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        // Можеш забрати дефолтні джерела, щоб він не шукав нічого в bin
        cfg.Sources.Clear();

        cfg.SetBasePath(configRoot)
           .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
           .AddJsonFile($"appsettings.{ctx.HostingEnvironment.EnvironmentName}.json",
                        optional: true, reloadOnChange: true)
           .AddEnvironmentVariables()
           .AddCommandLine(args);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddApplication();
        services.AddInfrastructure(ctx.Configuration);
    })
    .Build();

// для перевірки:
Console.WriteLine("Content root = " + host.Services
    .GetRequiredService<IHostEnvironment>().ContentRootPath);

var pipeline = host.Services.GetRequiredService<ITripEtlPipelineService>();
var stats = await pipeline.RunAsync();
