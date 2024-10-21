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
            try
            {
                if (args.Length == 0)
                {
                    Console.WriteLine("Usage: LogFileMerge2 <folderPath>");
                    return;
                }

                string folderPath = args[0];

                if (Directory.Exists(folderPath))
                {
                    Console.WriteLine($"Processing log files in folder: {folderPath}");
                    string[] files = Directory.GetFiles(folderPath);
                    Stopwatch totalStopwatch = new Stopwatch();
                    totalStopwatch.Start();

                    var fileContents = new BlockingCollection<(char[] Buffer, int Length)>(boundedCapacity: 100);
                    var consumerResults = new ChunkedList<(DateTime Timestamp, string Message)>[ConsumerCount];

                    Task producerTask = Task.Run(() => Producer(files, fileContents));
                    Task[] consumerTasks = new Task[ConsumerCount];
                    for (int i = 0; i < ConsumerCount; i++)
                    {
                        int consumerIndex = i;
                        consumerTasks[i] = Task.Run(() => Consumer(fileContents, consumerResults, consumerIndex));
                    }

                    producerTask.Wait();
                    fileContents.CompleteAdding();
                    Task.WaitAll(consumerTasks);

                    var finalResult = MergeSortedChunks(consumerResults);

                    string outputPath = Path.Combine(folderPath, "output.log");
                    using (StreamWriter writer = new StreamWriter(outputPath, false, Encoding.UTF8, 8 * 1024 * 1024))
                    {
                        foreach (var entry in finalResult)
                        {
                            writer.WriteLine(entry.Message.Trim());
                        }
                    }

                    totalStopwatch.Stop();

                    Console.WriteLine($"Log files processed: {files.Length}");
                    Console.WriteLine($"Total log entries: {finalResult.Count}");
                    Console.WriteLine($"Sorted log entries written to: {outputPath}");
                    Console.WriteLine($"Total processing time: {totalStopwatch.ElapsedMilliseconds} ms");
                }
                else
                {
                    Console.WriteLine($"Folder {folderPath} does not exist.");
                }
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.WriteLine($"Error: The specified directory was not found. {ex.Message}");
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error: An I/O error occurred. {ex.Message}");
            }
            catch (UnauthorizedAccessException ex)
            {
                Console.WriteLine($"Error: Access to the path is denied. {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("Press any key to exit.");
                Console.ReadKey();
            }
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

        private static void Consumer(BlockingCollection<(char[] Buffer, int Length)> fileContents, ChunkedList<(DateTime Timestamp, string Message)>[] consumerResults, int consumerIndex)
        {
            var localLogEntries = new ChunkedList<(DateTime Timestamp, string Message)>(10000);

            foreach (var item in fileContents.GetConsumingEnumerable())
            {
                var (buffer, length) = item;
                try
                {
                    ParseBuffer(buffer, length, localLogEntries);
                }
                finally
                {
                    ArrayPool<char>.Shared.Return(buffer);
                }
            }

            localLogEntries.SortAllChunks(Comparer<(DateTime Timestamp, string Message)>.Create((x, y) => x.Timestamp.CompareTo(y.Timestamp)));
            consumerResults[consumerIndex] = localLogEntries;
        }

        private static void ParseBuffer(char[] buffer, int length, ChunkedList<(DateTime Timestamp, string Message)> logEntries)
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
                    var parsedEntry = ParseLogEntry(lineSpan);
                    if (parsedEntry.HasValue)
                    {
                        // If there is a previous log entry, add it to the collection
                        if (logEntryTimestamp.HasValue)
                        {
                            var logEntrySpan = span.Slice(logEntryStart, logEntryEnd - logEntryStart).Trim();
                            logEntries.Add((logEntryTimestamp.Value, logEntrySpan.ToString()));
                        }

                        // Start a new log entry
                        logEntryTimestamp = parsedEntry.Value.Timestamp;
                        logEntryStart = start;
                        logEntryEnd = start + end + 1;
                    }
                    else if (logEntryTimestamp.HasValue)
                    {
                        // If the line is part of the previous log entry, extend the end position
                        logEntryEnd = start + end + 1;
                    }
                }

                start += end + 1;
            }

            // Add the last log entry if it exists
            if (logEntryTimestamp.HasValue)
            {
                var logEntrySpan = span.Slice(logEntryStart, logEntryEnd - logEntryStart).Trim();
                logEntries.Add((logEntryTimestamp.Value, logEntrySpan.ToString()));
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
                    return (timestamp, lineSpan.ToString());
                }
            }

            return null;
        }

        private static List<(DateTime Timestamp, string Message)> MergeSortedChunks(ChunkedList<(DateTime Timestamp, string Message)>[] sortedChunkLists)
        {
            // Calculate the total size of all chunks combined
            int totalSize = sortedChunkLists.Sum(chunkedList => chunkedList.GetAllItems().Count());
            var finalResult = new List<(DateTime Timestamp, string Message)>(totalSize);

            var priorityQueue = new PriorityQueue<(DateTime Timestamp, string Message, IEnumerator<(DateTime Timestamp, string Message)>), DateTime>(
                Comparer<DateTime>.Create((x, y) => x.CompareTo(y))
            );

            // Initialize the priority queue with the first element of each chunk from all ChunkedLists
            foreach (var chunkedList in sortedChunkLists)
            {
                foreach (var chunk in chunkedList.GetChunks())
                {
                    var enumerator = chunk.GetEnumerator();
                    if (enumerator.MoveNext())
                    {
                        priorityQueue.Enqueue((enumerator.Current.Timestamp, enumerator.Current.Message, enumerator), enumerator.Current.Timestamp);
                    }
                }
            }

            while (priorityQueue.Count > 0)
            {
                var (timestamp, message, enumerator) = priorityQueue.Dequeue();
                finalResult.Add((timestamp, message));

                if (enumerator.MoveNext())
                {
                    priorityQueue.Enqueue((enumerator.Current.Timestamp, enumerator.Current.Message, enumerator), enumerator.Current.Timestamp);
                }
            }

            return finalResult;
        }
    }
}
