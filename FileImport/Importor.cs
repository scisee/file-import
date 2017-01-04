using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileImport
{
    public class Importor
    {
        public string FileName { get; set; }
        
        private ConcurrentQueue<string> readQueue = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> saveQueue = new ConcurrentQueue<string>();

        private const int bufferSize = 262144;
        private const int fileReadDelays = 600;
        private const int reportEveryLines = 1000;
        private const int linesInMemThrottle = 2500;
        private const int batch = 1000;

        private bool fileReadCompleted;
        private bool dataSaveCompleted;

        private readonly object fileReadLock = new object();
        private readonly object dataSaveLock = new object();

        public string Import(string fileName)
        {
            lock (fileReadLock)
            {
                fileReadCompleted = false;
            }

            lock (dataSaveLock)
            {
                dataSaveCompleted = false;
            }

            ReadFile(fileReadDelays, fileName, reportEveryLines, linesInMemThrottle);
            var result = SaveData(batch);
            return result;
            //WriteLog()
        }

        public void ReadFile(int fileReadDelays, string fileName, int reportEveryLines, int linesInMemThrottle)
        {
            var totalLine = 0;

            try
            {
                using (var fs = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var bs = new BufferedStream(fs, bufferSize))
                using (var sr = new StreamReader(bs))
                {
                    string line = null;

                    // Loop read file until no more lines to read
                    // but will do some throttle to make sure not to flush all data at once to the queue
                    while ((line = sr.ReadLine()) != null)
                    {
                        totalLine++;

                        if (totalLine % reportEveryLines == 0)
                        {
                            //LogInfo(null, string.Format("Lines Read = {0}, in Mem {1}", totalLine, linesRead.Count),
                            //    CurrentClass, currentMethod);

                            if ((readQueue.Count > 2 * linesInMemThrottle))
                            {
                                while (readQueue.Count > linesInMemThrottle)
                                {
                                    Thread.Sleep(fileReadDelays);
                                }
                            }
                        }

                        readQueue.Enqueue(line);
                    }

                    //LogInfo(null, string.Format("Lines Read = {0}, in Mem {1}", totalLine, linesRead.Count),
                    //    CurrentClass, currentMethod);
                }
            }
            catch (Exception ex)
            {
                saveQueue.Enqueue(string.Format("File read failed: {0}, process forcely terminated", ex.Message));
            }
            finally
            {
                // Tell other that the file read is completed
                lock (fileReadLock)
                {
                    fileReadCompleted = true;
                }
            }
        }

        public string SaveData(int batch)
        {
            var waitCount = 0;
            var lines = new List<string>();

            try
            {
                while (true)
                {
                    if (readQueue.IsEmpty)
                    {
                        if (fileReadCompleted)
                        {
                            break;
                        }
                        else
                        {
                            //LogInfo(null, string.Format("[{0}] {1} Waiting for File Read... ",
                            //    threadName, waitCount1), CurrentClass, currentMethod);
                            waitCount++;
                            Thread.Sleep(1000);
                            continue;
                        }
                    }

                    // Once linesRead has data, just dequeue it
                    // if cannot dequeue, report problem and keep looping 
                    // (this might be because other tasks(if any) may have already dequeue the available data
                    string first = null;

                    if (!readQueue.TryDequeue(out first))
                    {
                        //LogInfo(null, string.Format("[{0}] {1} problem reading ", threadName,
                        //    waitCount0), CurrentClass, currentMethod);
                        continue;
                    }

                    //if (firstLine)
                    //{
                    //    //cpHandler.SetHeader(first);
                    //    firstLine = false;
                    //    continue;
                    //}

                    // Add to a batch to process
                    lines.Add(first);

                    // When lines to process is greater than defined batch
                    // save it as a batch and collect the result
                    if (lines.Count > batch)
                    {
                        // call store proc
                        lines.ForEach(_line => saveQueue.Enqueue(_line));
                        lines.Clear();
                    }
                }

                if (lines.Any())
                {
                    // call store proc
                    lines.ForEach(_line => saveQueue.Enqueue(_line));
                    lines.Clear();
                }
            }
            catch (Exception ex)
            {
                saveQueue.Enqueue(string.Format("Saving Data Failed: {0}, Process Forcely Terminated", ex.Message));
            }
            finally
            {
                lock (dataSaveLock)
                {
                    dataSaveCompleted = true;
                }
            }

            return string.Join(Environment.NewLine, saveQueue.ToArray());
        }
    }
}
