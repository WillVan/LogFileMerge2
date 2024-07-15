using System.Collections.Generic;

public class ChunkedList<T>
{
    private readonly List<List<T>> chunks = new List<List<T>>(1000);
    private readonly int chunkCapacity;
    private int currentChunkIndex = -1;

    public ChunkedList(int chunkCapacity)
    {
        this.chunkCapacity = chunkCapacity;
    }

    public void Add(T item)
    {
        if (currentChunkIndex == -1 || chunks[currentChunkIndex].Count == chunkCapacity)
        {
            var newChunk = new List<T>(chunkCapacity);
            chunks.Add(newChunk);
            currentChunkIndex++;
        }

        chunks[currentChunkIndex].Add(item);
    }

    public IEnumerable<T> GetAllItems()
    {
        foreach (var chunk in chunks)
        {
            foreach (var item in chunk)
            {
                yield return item;
            }
        }
    }

    public void SortAllChunks(IComparer<T> comparer)
    {
        foreach (var chunk in chunks)
        {
            chunk.Sort(comparer);
        }
    }
}
