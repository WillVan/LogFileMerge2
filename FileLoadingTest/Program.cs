using System;
using System.Buffers;
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
        private static readonly int ConsumerCount = Environment.ProcessorCount;

        static void Main(string[] args)
        {
            // Check if Server GC is enabled
            Console.WriteLine($"Server GC is {(GCSettings.IsServerGC ? "enabled" : "disabled")}.");

            string folderPath = @"C:\largetest\";

            if (Directory.Exists(folderPath))
            {
                string[] files = Directory.GetFiles(folderPath);
                Stopwatch totalStopwatch = new Stopwatch();
                totalStopwatch.Start();

                var logEntries = new ConcurrentBag<(DateTime Timestamp, string Message)>();
                var fileContents = new BlockingCollection<(char[] Buffer, int Length)>(boundedCapacity: 100);

                // Start producer task
                Task producerTask = Task.Run(() => Producer(files, fileContents));

                // Start consumer tasks
                Task[] consumerTasks = new Task[ConsumerCount];
                for (int i = 0; i < ConsumerCount; i++)
                {
                    consumerTasks[i] = Task.Run(() => Consumer(fileContents, logEntries));
                }

                // Wait for producer to finish
                producerTask.Wait();

                // Signal consumers to complete
                fileContents.CompleteAdding();

                // Wait for all consumers to finish
                Task.WaitAll(consumerTasks);

                // Sort log entries
                Stopwatch sortStopwatch = new Stopwatch();
                sortStopwatch.Start();

                var sortedLogEntries = logEntries.AsParallel().OrderBy(entry => entry.Timestamp).ToArray();

                sortStopwatch.Stop();

                // Write sorted log entries to file
                Stopwatch writeStopwatch = new Stopwatch();
                writeStopwatch.Start();

                string outputPath = @"c:\output\output.log";
                Directory.CreateDirectory(Path.GetDirectoryName(outputPath));
                using (StreamWriter writer = new StreamWriter(outputPath, false, Encoding.UTF8, 8 * 1024 * 1024)) // 8MB buffer size
                {
                    foreach (var entry in sortedLogEntries)
                    {
                        writer.WriteLine(entry.Message);
                    }
                }

                writeStopwatch.Stop();
                totalStopwatch.Stop();

                // Print timings and other information
                Console.WriteLine($"Sorting time: {sortStopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine($"Writing time: {writeStopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine($"Total time: {totalStopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine($"Total files: {files.Length}");
                Console.WriteLine($"Total log entries: {logEntries.Count}");
                Console.WriteLine($"Sorted log entries written to {outputPath}");
            }
            else
            {
                Console.WriteLine($"Folder {folderPath} does not exist.");
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static void Producer(string[] files, BlockingCollection<(char[] Buffer, int Length)> fileContents)
        {
            foreach (string file in files)
            {
                try
                {
                    char[] buffer = ArrayPool<char>.Shared.Rent((int)new FileInfo(file).Length);
                    int charsRead;
                    using (StreamReader reader = new StreamReader(file, Encoding.UTF8))
                    {
                        charsRead = reader.Read(buffer, 0, buffer.Length);
                    }
                    fileContents.Add((buffer, charsRead));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not read file {file}: {ex.Message}");
                }
            }
        }

        private static void Consumer(BlockingCollection<(char[] Buffer, int Length)> fileContents, ConcurrentBag<(DateTime Timestamp, string Message)> logEntries)
        {
            foreach (var item in fileContents.GetConsumingEnumerable())
            {
                var (buffer, length) = item;
                try
                {
                    ReadOnlySpan<char> span = buffer.AsSpan(0, length);
                    int start = 0;
                    DateTime? logEntryTimestamp = null;
                    int logEntryStart = 0;
                    int logEntryEnd = 0;

                    while (start < span.Length)
                    {
                        int end = span.Slice(start).IndexOf('\n');
                        if (end == -1)
                        {
                            end = span.Length - start;
                        }

                        ReadOnlySpan<char> lineSpan = span.Slice(start, end).Trim();
                        if (!lineSpan.IsEmpty)
                        {
                            if (logEntryTimestamp.HasValue)
                            {
                                if (!lineSpan.StartsWith("at "))
                                {
                                    // Add the previous log entry to the collection
                                    var logEntrySpan = span.Slice(logEntryStart, logEntryEnd - logEntryStart);
                                    logEntries.Add((logEntryTimestamp.Value, logEntrySpan.ToString()));
                                    logEntryTimestamp = null;
                                }
                                else
                                {
                                    logEntryEnd = start + end + 1;
                                }
                            }

                            if (!logEntryTimestamp.HasValue)
                            {
                                var parsedEntry = ParseLogEntry(lineSpan);
                                if (parsedEntry.HasValue)
                                {
                                    logEntryTimestamp = parsedEntry.Value.Timestamp;
                                    logEntryStart = start;
                                    logEntryEnd = start + end + 1;
                                }
                            }
                        }

                        start += end + 1;
                    }

                    // Add the last log entry if it exists
                    if (logEntryTimestamp.HasValue)
                    {
                        var logEntrySpan = span.Slice(logEntryStart, logEntryEnd - logEntryStart);
                        logEntries.Add((logEntryTimestamp.Value, logEntrySpan.ToString()));
                    }
                }
                finally
                {
                    ArrayPool<char>.Shared.Return(buffer); // Clear the array to avoid holding onto references
                }
            }
        }

        private static (DateTime Timestamp, string Message)? ParseLogEntry(ReadOnlySpan<char> lineSpan)
        {
            int separatorIndex = lineSpan.IndexOf(" - ");
            if (separatorIndex > 0)
            {
                ReadOnlySpan<char> timestampSpan = lineSpan.Slice(0, separatorIndex);

                if (DateTime.TryParse(timestampSpan, out DateTime timestamp))
                {
                    return (timestamp, lineSpan.ToString()); // Include the complete line in the message
                }
            }

            return null;
        }
    }
}
