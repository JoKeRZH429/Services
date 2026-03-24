

using Amazon.S3;
using Amazon.S3.Model;
using GenOnlineService;
using Microsoft.EntityFrameworkCore;
using MySqlX.XDevAPI.Common;
using Sentry.Protocol;
using System.Collections.Concurrent;
using System.Net;

public enum ES3UploadType
{
    Screenshot = 0,
    Replay = 1
}

public enum ES3QueueUploadResult
{
    Success,
    Failed_InvalidUploadType,
    Failed_InvalidImage_Size,
    Failed_InvalidImage_Header,
    Failed_QueueFull
}

class S3QueuedUploadEntry
{
    public ES3UploadType m_uploadType { get; init; }
    public byte[] m_FileData { get; init; }
    public int m_slotIndexInLobby { get; init; }
    public UInt64 m_MatchID { get; init; }
    public Int64 m_UserID { get; init; }
    public EScreenshotType m_screenshotTypeIfScreenshot { get; init; }

    public int m_RetryCount { get; set; } = 0;
    public long m_NextRetryTime { get; set; } = 0;

    public S3QueuedUploadEntry(ES3UploadType uploadType, byte[] fileBytes, UInt64 match_id, Int64 user_id, int slotIndexInLobby, EScreenshotType screenshotTypeIfScreenshot)
    {
        m_uploadType = uploadType;
        m_FileData = fileBytes;
        m_MatchID = match_id;
        m_UserID = user_id;
        m_screenshotTypeIfScreenshot = screenshotTypeIfScreenshot;
        m_slotIndexInLobby = slotIndexInLobby;
    }
}

static class BackgroundS3Uploader
{
    private static ConcurrentQueue<S3QueuedUploadEntry> m_queueUploads = new();

    // Entries waiting for their retry backoff delay — kept separate so they don't block
    // new uploads sitting behind them in the main queue.
    private static readonly List<S3QueuedUploadEntry> m_retryList = new();
    private static readonly object m_retryLock = new();

    // Running total of bytes currently held across the queue and retry list.
    // Prevents unbounded memory growth when uploads back up.
    private static Int64 g_TotalQueuedBytes = 0;
    private const Int64 MaxQueuedBytes = 2L * (1024 * 1024 * 1024); // 2GB

    private static AmazonS3Client? g_S3Client = null;
    private static readonly object m_clientLock = new();

    private static Int64 g_LastUpload = -1;

    private static Thread g_BackgroundThread = null;
    private static volatile bool g_bShutdownRequested = false;

    public static void Initialize()
    {
        g_BackgroundThread = new Thread(new ThreadStart(TickThreaded));
        g_BackgroundThread.Start();
    }

    public static void Shutdown()
    {
        g_bShutdownRequested = true;
        g_BackgroundThread.Join();
    }

    public static void TickThreaded() // This is called on a thread, and uploads one file at a time, and max of 10 per second
    {
        while (!g_bShutdownRequested)
        {
            try
            {
                // Move any retry entries whose backoff has expired back into the main queue.
                lock (m_retryLock)
                {
                    long now = Environment.TickCount64;
                    for (int i = m_retryList.Count - 1; i >= 0; i--)
                    {
                        if (m_retryList[i].m_NextRetryTime <= now)
                        {
                            m_queueUploads.Enqueue(m_retryList[i]);
                            m_retryList.RemoveAt(i);
                        }
                    }
                }

                // do we have something to upload?
                if (!m_queueUploads.IsEmpty)
                {
                    if (Environment.TickCount64 - g_LastUpload > 100) // max one file per 100ms
                    {
                        // queue the next thing
                        if (m_queueUploads.TryDequeue(out S3QueuedUploadEntry entry))
                        {
                            DoUpload(entry).GetAwaiter().GetResult();
                            g_LastUpload = Environment.TickCount64;
                        }
                    }
                }
                else
                {
                    // just sleep for a second
                    System.Threading.Thread.Sleep(1000);
                }
            }
            catch (Exception ex)
            {
                // Never let an unexpected exception kill the upload thread.
                Console.WriteLine($"Unexpected error in upload thread: {ex}");
                SentrySdk.CaptureException(ex);
                System.Threading.Thread.Sleep(1000);
            }

            System.Threading.Thread.Sleep(1);
        }
    }

    private static async Task DoUpload(S3QueuedUploadEntry entry)
    {
        {
            DateTime utcNow = DateTime.UtcNow;
            int hour = utcNow.Hour;
            int minute = utcNow.Minute;

            string strPerMatchUserIDKey = Helpers.ComputeMD5Hash(String.Format("{0}_{1}", entry.m_MatchID, entry.m_UserID)); ;
            string strFileName = null;
            EMetadataFileType fileType = EMetadataFileType.UNKNOWN;


			if (entry.m_uploadType == ES3UploadType.Screenshot)
            {
                fileType = EMetadataFileType.FILE_TYPE_SCREENSHOT;

				string screenshotTypePrefix = "screenshot";
                if (entry.m_screenshotTypeIfScreenshot == EScreenshotType.SCREENSHOT_TYPE_LOADSCREEN)
                {
                    screenshotTypePrefix = "loadscreen";
                }
                else if (entry.m_screenshotTypeIfScreenshot == EScreenshotType.SCREENSHOT_TYPE_GAMEPLAY)
                {
                    screenshotTypePrefix = "gameplay";
                }
                else if (entry.m_screenshotTypeIfScreenshot == EScreenshotType.SCREENSHOT_TYPE_SCORESCREEN)
                {
                    screenshotTypePrefix = "scorescreen";
                }

                strFileName = String.Format("screenshot_{0}_{1}_{2}.jpg", screenshotTypePrefix, hour, minute);
            }
            else if (entry.m_uploadType == ES3UploadType.Replay)
            {
				fileType = EMetadataFileType.FILE_TYPE_REPLAY;

				strFileName = String.Format("match_{0}_user_{1}_replay.rep", entry.m_MatchID, strPerMatchUserIDKey);
            }

            if (strFileName == null)
            {
                return;
            }

			string objectKey = String.Format("match_{0}/user_{1}/{2}", entry.m_MatchID, strPerMatchUserIDKey, strFileName);

			// get config
			if (Program.g_Config == null)
            {
                return;
            }

            IConfiguration? matchdataSettings = Program.g_Config.GetSection("MatchData");

            if (matchdataSettings == null)
            {
                return;
            }

            // is upload enabled?
            bool bUploadMatchData = matchdataSettings.GetValue<bool>("upload_match_data");
            if (!bUploadMatchData)
            {
                return;
            }

            string? strS3AccessKey = matchdataSettings.GetValue<string>("s3_access_key");
            string? strS3SecretKey = matchdataSettings.GetValue<string>("s3_secret_key");
            string? strS3BucketName = matchdataSettings.GetValue<string>("s3_bucket_name");
            string? strS3Endpoint = matchdataSettings.GetValue<string>("s3_endpoint");

            // now upload
            try
            {
                // Reuse a single S3 client for the lifetime of the process.
                // It is nulled out on non-throttle errors so it gets recreated if
                // credentials rotate or the connection pool goes bad.
                AmazonS3Client client;
                lock (m_clientLock)
                {
                    if (g_S3Client == null)
                    {
                        var s3Config = new AmazonS3Config
                        {
                            ServiceURL = strS3Endpoint,
                            ForcePathStyle = true, // Required for R2
                            Timeout = TimeSpan.FromSeconds(30),
                            MaxErrorRetry = 0 // we handle retries ourselves
                        };
                        g_S3Client = new AmazonS3Client(strS3AccessKey, strS3SecretKey, s3Config);
                    }
                    client = g_S3Client;
                }

                // Upload file
                using MemoryStream fileStream = new MemoryStream(entry.m_FileData);
                var putRequest = new PutObjectRequest
                {
                    BucketName = strS3BucketName,
                    Key = objectKey,
                    InputStream = fileStream,
                    DisablePayloadSigning = true,
                    DisableDefaultChecksumValidation = true,
                };

                var response = await client.PutObjectAsync(putRequest);
                Console.WriteLine($"SCREENSHOT uploaded successfully. {entry.m_FileData.Length} bytes. HTTP Status Code: {response.HttpStatusCode}");

				// store in DB
				using var scope = ServiceLocator.Services.CreateScope();
				var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<AppDbContext>>();
				await using var db = await factory.CreateDbContextAsync();
				await Database.MatchHistory.AttachMatchHistoryMetadata(db, entry.m_MatchID, entry.m_slotIndexInLobby, strFileName, fileType);

				// Entry is fully done — release its memory from the tracked total.
				Interlocked.Add(ref g_TotalQueuedBytes, -entry.m_FileData.Length);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading file: {ex.Message}");

				SentrySdk.CaptureException(ex);

                entry.m_RetryCount++;

                if (entry.m_RetryCount >= 5)
                {
                    Console.WriteLine($"Upload failed after 5 attempts, dropping entry (match {entry.m_MatchID}, user {entry.m_UserID}).");
					Interlocked.Add(ref g_TotalQueuedBytes, -entry.m_FileData.Length);
                    return;
                }

                // Exponential backoff: 2s, 4s, 8s, 16s
                long backoffMs = (long)Math.Pow(2, entry.m_RetryCount) * 1000L;

                // For 429 / S3 throttle responses, enforce a minimum 30s wait
                bool isThrottled = ex is AmazonS3Exception s3Ex &&
                    (s3Ex.StatusCode == HttpStatusCode.TooManyRequests ||
                     s3Ex.ErrorCode == "SlowDown" ||
                     s3Ex.ErrorCode == "ThrottlingException");

                if (isThrottled)
                    backoffMs = Math.Max(backoffMs, 30_000L);
                else
                {
                    // Non-throttle failure — the client itself may be broken (bad credentials,
                    // dead connection pool). Null it out so it gets recreated on the next attempt.
                    lock (m_clientLock)
                    {
                        g_S3Client?.Dispose();
                        g_S3Client = null;
                    }
                }

                entry.m_NextRetryTime = Environment.TickCount64 + backoffMs;

                Console.WriteLine($"Retrying upload in {backoffMs}ms (attempt {entry.m_RetryCount}/5, throttled={isThrottled}).");

				RequeueEntry(entry);
                return;
            }
        }
    }

    private static void RequeueEntry(S3QueuedUploadEntry entry)
    {
        lock (m_retryLock)
        {
            m_retryList.Add(entry);
        }
    }

    public static ES3QueueUploadResult QueueUpload(ES3UploadType uploadType, byte[] fileBytes, UInt64 match_id, Int64 user_id, int slotIndexInLobby, EScreenshotType screenshotTypeIfScreenshot)
    {
        if (uploadType == ES3UploadType.Screenshot)
        {
            if (fileBytes.Length < 3) // should have 3 bytes at least, thats the header size
            {
                return ES3QueueUploadResult.Failed_InvalidImage_Size;
            }
            
            if (fileBytes[0] != 0xFF || fileBytes[1] != 0xD8 || fileBytes[2] != 0xFF)
            {
                // must be jpg
                return ES3QueueUploadResult.Failed_InvalidImage_Header;
            }
        }
        else if (uploadType == ES3UploadType.Replay)
        {
            if (fileBytes.Length < 6) // should have 6 bytes at least, thats the header size
            {
                return ES3QueueUploadResult.Failed_InvalidImage_Size;
            }

            if (System.Text.Encoding.UTF8.GetString(fileBytes, 0, 6) != "GENREP")
            {
                // must be jpg
                return ES3QueueUploadResult.Failed_InvalidImage_Header;
            }
        }
        else
        {
            return ES3QueueUploadResult.Failed_InvalidUploadType;
        }

        // Reject if queueing this file would push total in-flight bytes over the cap.
        // This prevents runaway memory growth when uploads back up under load.
        if (Interlocked.Add(ref g_TotalQueuedBytes, fileBytes.Length) > MaxQueuedBytes)
        {
            Interlocked.Add(ref g_TotalQueuedBytes, -fileBytes.Length);
            Console.WriteLine($"[WARN] Upload queue full ({MaxQueuedBytes / 1024 / 1024} MB cap), dropping upload for match {match_id} user {user_id}.");
            return ES3QueueUploadResult.Failed_QueueFull;
        }

        // queue it
        S3QueuedUploadEntry newUploadEntry = new S3QueuedUploadEntry(uploadType, fileBytes, match_id, user_id, slotIndexInLobby, screenshotTypeIfScreenshot);
        m_queueUploads.Enqueue(newUploadEntry);

        return ES3QueueUploadResult.Success;
    }
}