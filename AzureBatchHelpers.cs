using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Azure.Storage;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Configuration;

namespace ConsoleApp1
{
    internal static class AzureBatchHelpers
    {
        internal static void CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration, IConfigurationRoot config)
        {
            // Batch resource settings
            var PoolId = config.GetSection("PoolId").Value;
            var PoolNodeCount = int.Parse(config.GetSection("PoolNodeCount").Value ?? "2");
            var PoolVMSize = config.GetSection("PoolVMSize").Value;

            try
            {
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: PoolNodeCount,
                    virtualMachineSize: PoolVMSize,
                    virtualMachineConfiguration: vmConfiguration);

                pool.Commit();
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

        internal static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.windows amd64");
        }

        internal static ImageReference CreateImageReference(IConfigurationRoot config)
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
        internal static ResourceFile UploadFileToContainer(BlobContainerClient containerClient, string containerName, string filePath, string? storedPolicyName = null)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);
            string blobName = Path.GetFileName(filePath);
            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            var blobClient = containerClient.GetBlobClient(blobName);
            blobClient.Upload(filePath, true);

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

    }
}
