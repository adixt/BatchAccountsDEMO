using ConsoleApp1;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

var configuration = new ConfigurationBuilder()
        .AddJsonFile($"appsettings.json");
var config = configuration.Build();

// Update the Batch and Storage account credential strings below with the values unique to your accounts.
// Batch account credentials
var BatchAccountName = config.GetSection("BatchAccountName").Value ?? "";
var BatchAccountKey = config.GetSection("BatchAccountKey").Value ?? "";
var BatchAccountUrl = config.GetSection("BatchAccountUrl").Value ?? "";
// Storage account credentials
var StorageAccountName = config.GetSection("StorageAccountName").Value ?? "";
var StorageAccountKey = config.GetSection("StorageAccountKey").Value ?? "";
var JobId = config.GetSection("JobId").Value ?? "";
var PoolId = config.GetSection("PoolId").Value ?? "";
var InputContainerName = config.GetSection("InputContainerName").Value ?? "";

if (string.IsNullOrEmpty(BatchAccountName) ||
               string.IsNullOrEmpty(BatchAccountKey) ||
               string.IsNullOrEmpty(BatchAccountUrl) ||
               string.IsNullOrEmpty(StorageAccountName) ||
               string.IsNullOrEmpty(StorageAccountKey))
{
    throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
}

try
{
    Console.WriteLine("Sample start: {0}", DateTime.Now);
    Console.WriteLine();
    var timer = new Stopwatch();
    timer.Start();

    // Create the blob client, for use in obtaining references to blob storage containers
    var blobServiceClient = AzureBatchHelpers.GetBlobServiceClient(StorageAccountName, StorageAccountKey);

    var containerClient = blobServiceClient.GetBlobContainerClient(InputContainerName);
    await containerClient.CreateIfNotExistsAsync();

    var inputFiles = await AzureBatchHelpers.UploadInputFiles(InputContainerName, containerClient);

    // Get a Batch client using account creds
    var cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

    using var batchClient = BatchClient.Open(cred);
    await AzureBatchHelpers.CreateBatchPool(config, PoolId, batchClient);
    await AzureBatchHelpers.CreateBatchJob(JobId, PoolId, batchClient);

    var tasks = AzureBatchHelpers.CreateTasksForJob(JobId, inputFiles);

    await AzureBatchHelpers.WaitForTasksCompletion(JobId, batchClient, tasks);

    AzureBatchHelpers.PrintTasksOutput(JobId, batchClient);

    AzureBatchHelpers.PrintTotalOperationTime(timer);

    await AzureBatchHelpers.CleanUpStorage(InputContainerName, containerClient);
    await AzureBatchHelpers.CleanUpJob(JobId, batchClient);
    await AzureBatchHelpers.CleanUpTasks(PoolId, batchClient);
}
finally
{
    Console.WriteLine();
    Console.WriteLine("Sample complete, hit ENTER to exit...");
    Console.ReadLine();
}
