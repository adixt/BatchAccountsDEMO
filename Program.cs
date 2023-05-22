using ConsoleApp1;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

Console.WriteLine("Hello, World!");

var configuration = new ConfigurationBuilder()
        .AddJsonFile($"appsettings.json");

var config = configuration.Build();
// Update the Batch and Storage account credential strings below with the values unique to your accounts.
// These are used when constructing connection strings for the Batch and Storage client objects.

// Batch account credentials
var BatchAccountName = config.GetSection("BatchAccountName").Value;
var BatchAccountKey = config.GetSection("BatchAccountKey").Value;
var BatchAccountUrl = config.GetSection("BatchAccountUrl").Value;

// Storage account credentials
var StorageAccountName = config.GetSection("StorageAccountName").Value;
var StorageAccountKey = config.GetSection("StorageAccountKey").Value;

var JobId = config.GetSection("JobId").Value; 
var PoolId = config.GetSection("PoolId").Value;

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

    // Use the blob client to create the input container in Azure Storage 
    const string inputContainerName = "input";
    var containerClient = blobServiceClient.GetBlobContainerClient(inputContainerName);
    containerClient.CreateIfNotExistsAsync().Wait();

    // The collection of data files that are to be processed by the tasks
    var inputFilePaths = new List<string>
                {
                    "taskdata0.txt",
                    "taskdata1.txt",
                    "taskdata2.txt"
                };

    // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
    // executed on the compute nodes within the pool.
    var inputFiles = new List<ResourceFile>();

    foreach (var filePath in inputFilePaths)
    {
        inputFiles.Add(AzureBatchHelpers.UploadFileToContainer(containerClient, inputContainerName, filePath));
    }

    // Get a Batch client using account creds
    var cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

    using var batchClient = BatchClient.Open(cred);
    Console.WriteLine("Creating pool [{0}]...", PoolId);

    // Create a Windows Server image, VM configuration, Batch pool
    var imageReference = AzureBatchHelpers.CreateImageReference(config);
    var vmConfiguration = AzureBatchHelpers.CreateVirtualMachineConfiguration(imageReference);
    AzureBatchHelpers.CreateBatchPool(batchClient, vmConfiguration, config);

    // Create a Batch job
    Console.WriteLine("Creating job [{0}]...", JobId);

    try
    {
        var job = batchClient.JobOperations.CreateJob();
        job.Id = JobId;
        job.PoolInformation = new PoolInformation { PoolId = PoolId };
        job.Commit();
    }
    catch (BatchException be)
    {
        // Accept the specific error code JobExists as that is expected if the job already exists
        if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
        {
            Console.WriteLine("The job {0} already existed when we tried to create it", JobId);
        }
        else
        {
            throw; // Any other exception is unexpected
        }
    }

    // Create a collection to hold the tasks that we'll be adding to the job
    Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, JobId);
    var tasks = new List<CloudTask>();

    // Create each of the tasks to process one of the input files. 
    for (int i = 0; i < inputFiles.Count; i++)
    {
        var taskId = string.Format("Task{0}", i);
        var inputFilename = inputFiles[i].FilePath;
        var taskCommandLine = string.Format("cmd /c type {0} && mkdir C:\\test2 && echo 213 >> C:\\test2\\2.txt", inputFilename);

        var task = new CloudTask(taskId, taskCommandLine)
        {
            ResourceFiles = new List<ResourceFile> { inputFiles[i] }
        };
        tasks.Add(task);
    }

    // Add all tasks to the job.
    batchClient.JobOperations.AddTask(JobId, tasks);

    // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
    var timeout = TimeSpan.FromMinutes(30);
    Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

    var addedTasks = batchClient.JobOperations.ListTasks(JobId);
    batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);
    Console.WriteLine("All tasks reached state Completed.");

    // Print task output
    Console.WriteLine();
    Console.WriteLine("Printing task output...");

    var completedtasks = batchClient.JobOperations.ListTasks(JobId);
    foreach (CloudTask task in completedtasks)
    {
        string nodeId = string.Format(task.ComputeNodeInformation.ComputeNodeId);
        Console.WriteLine("Task: {0}", task.Id);
        Console.WriteLine("Node: {0}", nodeId);
        Console.WriteLine("Standard out:");
        Console.WriteLine(task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
    }

    // Print out some timing info
    timer.Stop();
    Console.WriteLine();
    Console.WriteLine("Sample end: {0}", DateTime.Now);
    Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

    // Clean up Storage resources
    containerClient.DeleteIfExistsAsync().Wait();
    Console.WriteLine("Container [{0}] deleted.", inputContainerName);

    // Clean up Batch resources (if the user so chooses)
    Console.WriteLine();
    Console.Write("Delete job? [yes] no: ");
    var response = Console.ReadLine()?.ToLower();
    if (response != "n" && response != "no")
    {
        batchClient.JobOperations.DeleteJob(JobId);
    }

    Console.Write("Delete pool? [yes] no: ");
    response = Console.ReadLine()?.ToLower();
    if (response != "n" && response != "no")
    {
        batchClient.PoolOperations.DeletePool(PoolId);
    }
}
finally
{
    Console.WriteLine();
    Console.WriteLine("Sample complete, hit ENTER to exit...");
    Console.ReadLine();
}
