using System;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileLoadingTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Check if Server GC is enabled
            Console.WriteLine($"Server GC is {(GCSettings.IsServerGC ? "enabled" : "disabled")}.");

            string folderPath = @"c:\LogFileMerge";

            if (Directory.Exists(folderPath))
            {
                string[] files = Directory.GetFiles(folderPath);
                Stopwatch totalStopwatch = new Stopwatch();
                Stopwatch readLineStopwatch = new Stopwatch();
                Stopwatch parseLogEntryStopwatch = new Stopwatch();
                totalStopwatch.Start();
                var logEntries = new ConcurrentBag<(DateTime Timestamp, string Message)>();
                int fileCount = 0;

                foreach (string file in files)
                {
                    try
                    {
                        // 64k buffer for file stream 
                        using (StreamReader reader = new StreamReader(file, Encoding.UTF8, true, 65536)) // 64k buffer
                        {
                            string line;
                            while (true)
                            {
                                readLineStopwatch.Start();
                                line = reader.ReadLine();
                                readLineStopwatch.Stop();

                                if (line == null)
                                    break;

                                // Parse the line
                                parseLogEntryStopwatch.Start();
                                var parsedEntry = ParseLogEntry(line);
                                parseLogEntryStopwatch.Stop();

                                if (parsedEntry.HasValue)
                                {
                                    logEntries.Add(parsedEntry.Value);
                                }
                            }
                        }
                        fileCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Could not read file {file}: {ex.Message}");
                    }
                }

                totalStopwatch.Stop();
                Console.WriteLine($"Total time: {totalStopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine($"Total files: {fileCount}");
                Console.WriteLine($"Total log entries: {logEntries.Count}");
                Console.WriteLine($"Total ReadLine time: {readLineStopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine($"Total ParseLogEntry time: {parseLogEntryStopwatch.ElapsedMilliseconds} ms");
            }
            else
            {
                Console.WriteLine($"Folder {folderPath} does not exist.");
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static (DateTime Timestamp, string Message)? ParseLogEntry(string line)
        {
            ReadOnlySpan<char> lineSpan = line.AsSpan();
            int separatorIndex = lineSpan.IndexOf(" - ".AsSpan());
            if (separatorIndex > 0)
            {
                ReadOnlySpan<char> timestampSpan = lineSpan.Slice(0, separatorIndex);
                ReadOnlySpan<char> messageSpan = lineSpan.Slice(separatorIndex + 3);

                // Convert ReadOnlySpan<char> to string
                string timestampString = timestampSpan.ToString();

                if (DateTime.TryParseExact(timestampString, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime timestamp))
                {
                    return (timestamp, messageSpan.ToString());
                }
            }

            return null;
        }
    }
}
