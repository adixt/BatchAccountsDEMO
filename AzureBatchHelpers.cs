using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Azure.Storage;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace ConsoleApp1
{
    internal static class AzureBatchHelpers
    {
        private static async Task CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration, IConfigurationRoot config)
        {
            // Batch resource settings
            var PoolId = config.GetSection("PoolId").Value ?? "";
            var PoolNodeCount = int.Parse(config.GetSection("PoolNodeCount").Value ?? "2");
            var PoolVMSize = config.GetSection("PoolVMSize").Value ?? "";

            try
            {
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: PoolNodeCount,
                    virtualMachineSize: PoolVMSize,
                    virtualMachineConfiguration: vmConfiguration);

                await pool.CommitAsync();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.windows amd64");
        }

        private static ImageReference CreateImageReference(IConfigurationRoot config)
        {
            return new ImageReference(
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: config.GetSection("VMImageSku").Value,
                version: "latest");
        }

        /// <summary>
        /// Creates a blob client
        /// </summary>
        /// <param name="storageAccountName">The name of the Storage Account</param>
        /// <param name="storageAccountKey">The key of the Storage Account</param>
        /// <returns></returns>
        internal static BlobServiceClient GetBlobServiceClient(string storageAccountName, string storageAccountKey)
        {
            var sharedKeyCredential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
            string blobUri = "https://" + storageAccountName + ".blob.core.windows.net";

            var blobServiceClient = new BlobServiceClient(new Uri(blobUri), sharedKeyCredential);
            return blobServiceClient;
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="containerClient">A <see cref="BlobContainerClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile instance representing the file within blob storage.</returns>
        private static async Task<ResourceFile> UploadFileToContainer(BlobContainerClient containerClient, string containerName, string filePath, string? storedPolicyName = null)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);
            var blobName = Path.GetFileName(filePath);
            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            var blobClient = containerClient.GetBlobClient(blobName);
            await blobClient.UploadAsync(filePath, true);

            // Set the expiry time and permissions for the blob shared access signature. 
            // In this case, no start time is specified, so the shared access signature 
            // becomes valid immediately
            // Check whether this BlobContainerClient object has been authorized with Shared Key.

            if (blobClient.CanGenerateSasUri)
            {
                // Create a SAS token
                var sasBuilder = new BlobSasBuilder()
                {
                    BlobContainerName = containerClient.Name,
                    BlobName = blobClient.Name,
                    Resource = "b"
                };

                if (storedPolicyName == null)
                {
                    sasBuilder.ExpiresOn = DateTimeOffset.UtcNow.AddHours(1);
                    sasBuilder.SetPermissions(BlobContainerSasPermissions.Read);
                }
                else
                {
                    sasBuilder.Identifier = storedPolicyName;
                }

                var sasUri = blobClient.GenerateSasUri(sasBuilder).ToString();
                return ResourceFile.FromUrl(sasUri, filePath);
            }
            else
            {
                throw new InvalidOperationException("BlobClient must be authorized with shared key credentials to create a service SAS.");
            }
        }


        internal static IEnumerable<CloudTask> CreateTasksForJob(string JobId, IEnumerable<ResourceFile> inputFiles)
        {
            // Create a collection to hold the tasks that we'll be adding to the job
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count(), JobId);
            var tasks = new List<CloudTask>();

            // Create each of the tasks to process one of the input files. 
            foreach (var file in inputFiles)
            {
                var taskId = string.Format("Task{0}", tasks.Count);
                var inputFilename = file.FilePath;
                var taskCommandLine = string.Format("cmd /c type {0}", inputFilename);

                var task = new CloudTask(taskId, taskCommandLine)
                {
                    ResourceFiles = new List<ResourceFile> { file }
                };
                tasks.Add(task);
            }

            return tasks;
        }

        internal static void PrintTasksOutput(string JobId, BatchClient batchClient)
        {
            // Print task output
            Console.WriteLine();
            Console.WriteLine("Printing task output...");

            var completedtasks = batchClient.JobOperations.ListTasks(JobId);
            foreach (var task in completedtasks)
            {
                var nodeId = string.Format(task.ComputeNodeInformation.ComputeNodeId);
                Console.WriteLine("Task: {0}", task.Id);
                Console.WriteLine("Node: {0}", nodeId);
                Console.WriteLine("Standard out:");
                Console.WriteLine(task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
            }
        }

        internal static async Task WaitForTasksCompletion(string? JobId, BatchClient batchClient, IEnumerable<CloudTask> tasks)
        {
            // Add all tasks to the job.
            await batchClient.JobOperations.AddTaskAsync(JobId, tasks);

            // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
            var timeout = TimeSpan.FromMinutes(30);
            Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

            var addedTasks = batchClient.JobOperations.ListTasks(JobId);
            batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);
            Console.WriteLine("All tasks reached state Completed.");
        }


        internal static async Task CreateBatchPool(IConfigurationRoot config, string PoolId, BatchClient batchClient)
        {
            Console.WriteLine("Creating pool [{0}]...", PoolId);

            // Create a Windows Server image, VM configuration, Batch pool
            var imageReference = CreateImageReference(config);
            var vmConfiguration = CreateVirtualMachineConfiguration(imageReference);
            await CreateBatchPool(batchClient, vmConfiguration, config);
        }

        internal static async Task CreateBatchJob(string JobId, string PoolId, BatchClient batchClient)
        {
            // Create a Batch job
            Console.WriteLine("Creating job [{0}]...", JobId);

            try
            {
                var job = batchClient.JobOperations.CreateJob();
                job.Id = JobId;
                job.PoolInformation = new PoolInformation { PoolId = PoolId };
                await job.CommitAsync();
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
        }

        internal static async Task CleanUpStorage(string InputContainerName, BlobContainerClient containerClient)
        {

            // Clean up Storage resources
            await containerClient.DeleteIfExistsAsync();
            Console.WriteLine("Container [{0}] deleted.", InputContainerName);
        }

        internal static async Task CleanUpJob(string JobId, BatchClient batchClient)
        {
            // Clean up Batch resources (if the user so chooses)
            Console.WriteLine();
            Console.Write("Delete job? [yes] no: ");
            var response = Console.ReadLine()?.ToLower();
            if (response != "n" && response != "no")
            {
                await batchClient.JobOperations.DeleteJobAsync(JobId);
            }
        }

        internal static async Task CleanUpTasks(string PoolId, BatchClient batchClient)
        {
            Console.Write("Delete pool? [yes] no: ");
            var response = Console.ReadLine()?.ToLower();
            if (response != "n" && response != "no")
            {
                await batchClient.PoolOperations.DeletePoolAsync(PoolId);
            }
        }

        internal static void PrintTotalOperationTime(Stopwatch timer)
        {
            // Print out some timing info
            timer.Stop();
            Console.WriteLine();
            Console.WriteLine("Sample end: {0}", DateTime.Now);
            Console.WriteLine("Elapsed time: {0}", timer.Elapsed);
        }

        internal static async Task<IEnumerable<ResourceFile>> UploadInputFiles(string InputContainerName, BlobContainerClient containerClient)
        {
            var inputFiles = new List<ResourceFile>();
            // The collection of data files that are to be processed by the tasks
            var inputFilePaths = new List<string>
                {
                    "taskdata0.txt",
                    "taskdata1.txt",
                    "taskdata2.txt"
                };

            // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
            // executed on the compute nodes within the pool.
            foreach (var filePath in inputFilePaths)
            {
                inputFiles.Add(await UploadFileToContainer(containerClient, InputContainerName, filePath));
            }
            return inputFiles;
        }

    }
}
