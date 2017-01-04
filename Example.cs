using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ELMCommonLibs.CommercialPositionManagement;
using ELMCommonLibs.Common;
using ELMCommonLibs.ConfigurationHandler;
using ELMCommonLibs.EmailDataTransferReference;
using ELMDataObject;
using ELMDataObject.Common;
using Ionic.Zip;
using Reuters.Cpfg.Aaa.Cpac.WebServices.RoleAndSettingWebService;
using ThomsonReuters.AAA.ECSOOrderHandler.TaskScheduler;
using ThomsonReuters.ECSO.CommonLibs.Stubs;
using ThomsonReuters.ECSO.CommonLibs.Utils;

namespace ThomsonReuters.AAA.ECSOOrderHandler.TaskELM
{
    public class CommercialPositionLoadTask : ECSOTask
    {
        public Type CurrentClass = typeof(CommercialPositionLoadTask);

        private const string FtpServerKey = "FtpServer";
        private const string FtpUserKey = "FtpUser";
        private const string FtpPasswordKey = "FtpPassword";
        private const string FtpDirectoryKey = "FtpDirectory";
        private const string FtpArchiveDirectoryKey = "FtpArchiveDirectory";
        private const string LocalBaseDirectoryKey = "LocalBaseDirectory";
        private const string LastTimeStampFileKey = "LastTimeStampFile";

        private const string CommercialPositionDirectoryName = "CommercialPosition";
        private const string CommercialPositionZipDirectoryName = "zip";
        private const string CommercialPositionInputDirectoryName = "Incoming";
        private const string CommercialPositionReportDirectoryName = "Archive";

        public const string CommercialPositionPattern = "yyyyMMddHHmmss";

        private Ftp ftp = null;

        public string FtpServer = null;
        public string FtpUser = null;
        public string FtpPassword = null;
        public string FtpDirectory = null;
        public string FtpArchiveDirectory = null;

        public string LocalBaseDirectory = null;
        public string LastTimeStampFile = null;

        public string CommercialPositionDirectory = null;

        public string CommercialPositionIncomingDirectory = null;

        public string CommercialPositionInputDirectory = null;

        public string CommercialPositionReportDirectory = null;


        public CommercialPositionLoadTask()
        {
            if (_createElmDbContext == null)
                _createElmDbContext = DBContextHandler.CreateLicenseContext;
        }

        protected CreateELMContext _createElmDbContext;

        public CommercialPositionLoadTask(CreateELMContext createElmContext)
        {
            _createElmDbContext = createElmContext;
        }

        public CreateELMContext MockCreateElmContext
        {
            set { _createElmDbContext = value; }
        }

        #region To user in unit test only
        public CreateELMContext ELMContent
        {
            get { return _createElmDbContext; }
            set { _createElmDbContext = value; }
        }
        public string _FtpServerKey
        {
            get { return FtpServerKey; }
        }
        public string _FtpUserKey
        {
            get { return FtpUserKey; }
        }
        public string _FtpPasswordKey
        {
            get { return FtpPasswordKey; }
        }
        public string _FtpDirectoryKey
        {
            get { return FtpDirectoryKey; }
        }
        public string _FtpArchiveDirectoryKey
        {
            get { return FtpArchiveDirectoryKey; }
        }
        public string _LocalBaseDirectoryKey
        {
            get { return LocalBaseDirectoryKey; }
        }
        public string _LastTimeStampFileKey
        {
            get { return LastTimeStampFileKey; }
        }
        public string _CommercialPositionDirectoryName
        {
            get { return CommercialPositionDirectoryName; }
        }
        public bool _FileReadCompleted
        {
            set { fileReadcompleted = value; }
        }
        public bool _Completed
        {
            get { return completed; }
            set { completed = value; }
        }
        public ConcurrentQueue<string> _LineRead
        {
            get { return linesRead; }
            set { linesRead = value; }
        }
        public ConcurrentQueue<string> _ResultQueue
        {
            get { return resultQueue; }
            set { resultQueue = value; }
        }
        #endregion


        [ExcludeFromCodeCoverage]
        private void TryGetParameter(string key, out string value)
        {
            string val = null;
            if (parameters.ContainsKey(key))
            {
                val = parameters[key];
            }

            value = val;
        }

        [ExcludeFromCodeCoverage]
        private void EnsureCreateDirectory(string directory)
        {
            if (Directory.Exists(directory)) return;
            Directory.CreateDirectory(directory);
        }

        [ExcludeFromCodeCoverage]
        private void EnsureDeleteDirectory(string directory)
        {
            if (!Directory.Exists(directory)) return;
            Directory.Delete(directory, true);
        }

        [ExcludeFromCodeCoverage]
        private void EnsureEmptyDirectory(string directory)
        {
            if (Directory.Exists(directory))
            {
                var directoryInfo = new DirectoryInfo(directory);
                foreach (var file in directoryInfo.GetFiles()) file.Delete();
                foreach (var subDirectory in directoryInfo.GetDirectories()) subDirectory.Delete(true);
            }
            else
            {
                Directory.CreateDirectory(directory);
            }
        }

        [ExcludeFromCodeCoverage]
        private void EnsureCreateFile(string file)
        {
            if (File.Exists(file)) return;
            using (var fs = File.Create(file))
            {
            }
        }

        [ExcludeFromCodeCoverage]
        private void EnsureDeleteFile(string file)
        {
            if (!File.Exists(file)) return;
            File.Delete(file);
        }

        [ExcludeFromCodeCoverage]
        public virtual DateTime GetLastCreateDateTime()
        {
            if (new FileInfo(LastTimeStampFile).Length == 0) return DateTime.MinValue;

            var lastCreateDateTime = DateTime.MinValue;
            var lastCreateDateTimeString = File.ReadAllText(LastTimeStampFile).Trim();

            return DateTime.TryParseExact(lastCreateDateTimeString, CommercialPositionPattern, null, DateTimeStyles.None,
                out lastCreateDateTime)
                ? lastCreateDateTime
                : DateTime.MinValue;
        }

        [ExcludeFromCodeCoverage]
        public virtual void UpdateLastCreateDateTime(string newCreateDateTimeString)
        {
            File.WriteAllText(LastTimeStampFile, newCreateDateTimeString);
        }

        [ExcludeFromCodeCoverage]
        public virtual void LogError(String UID, String msg, Type _type, MethodBase _method)
        {
            ECSOLogManager.Error(UID, msg, _type, _method);
        }

        [ExcludeFromCodeCoverage]
        public virtual void LogError(String UID, String msg, Type _type, MethodBase _method, Exception ex)
        {
            // ECSOLogManager.Error(UID, msg, _type, _method, ex);          
            // Added GMI, Edit on 5/30/2014
            ECSOLogManager.Error(UID, msg, _type, _method, ex,
                ELMExceptionhandler.DatabaseException(ex)
                    ? GMI_ECSO_Scenario.DBConnection
                    : GMI_ECSO_Scenario.Application);
        }

        [ExcludeFromCodeCoverage]
        public virtual void LogError(String UID, String msg, Type _type, MethodBase _method, Exception ex,
            GMI_ECSO_Scenario _scenario)
        {
            ECSOLogManager.Error(UID, msg, _type, _method, ex, _scenario);
        }

        [ExcludeFromCodeCoverage]
        public virtual void LogInfo(string uid, string msg, Type type, MethodBase method)
        {
            ECSOLogManager.Info(uid, msg, type, method);
        }

        [ExcludeFromCodeCoverage]
        public virtual Ftp GetFtp(string ftpServer, string ftpUser, string ftpPassword)
        {
            return new Ftp(ftpServer, ftpUser, ftpPassword);
        }

        [ExcludeFromCodeCoverage]
        public virtual string[] FtpDirectoryListSimple(string directory)
        {
            return ftp.DirectoryListSimple(directory);
        }

        [ExcludeFromCodeCoverage]
        public virtual bool FtpDownload(string remoteFile, string localFile)
        {
            return ftp.Download(remoteFile, localFile);
        }

        public virtual bool Initialize()
        {
            var currentMethod = MethodBase.GetCurrentMethod();

            try
            {
                TryGetParameter(FtpServerKey, out FtpServer);
                TryGetParameter(FtpUserKey, out FtpUser);
                TryGetParameter(FtpPasswordKey, out FtpPassword);
                TryGetParameter(FtpDirectoryKey, out FtpDirectory);
                TryGetParameter(FtpArchiveDirectoryKey, out FtpArchiveDirectory);
                TryGetParameter(LocalBaseDirectoryKey, out LocalBaseDirectory);
                TryGetParameter(LastTimeStampFileKey, out LastTimeStampFile);

                CommercialPositionDirectory = Path.Combine(LocalBaseDirectory, CommercialPositionDirectoryName);
                CommercialPositionIncomingDirectory = Path.Combine(CommercialPositionDirectory,
                    CommercialPositionZipDirectoryName);

                CommercialPositionInputDirectory = CommercialPositionIncomingDirectory;

                CommercialPositionReportDirectory = Path.Combine(CommercialPositionDirectory,
                    CommercialPositionReportDirectoryName);

                if (Util.ELM_IsAnyNullOrWhiteSpace(
                    FtpServer,
                    FtpUser,
                    FtpPassword,
                    FtpDirectory,
                    FtpArchiveDirectory,
                    LocalBaseDirectory,
                    LastTimeStampFile,
                    CommercialPositionDirectory,
                    CommercialPositionIncomingDirectory,
                    CommercialPositionInputDirectory,
                    CommercialPositionReportDirectory
                    ))
                {
                    LogError(null, "Required Parameter(s) can not be initialized", CurrentClass, currentMethod);
                    return false;
                }

                var info = string.Format(
                    "SFtpServer: {1}, {2}/{3}{0}FtpDirectory: {4}{0}FtpArchiveDirectory: {5}{0}LocalBaseDirectory: {6}{0}LastTimeStampFile: {7}{0}CommercialPositionDirectory: {8}{0}CommercialPositionZipDirectory: {9}{0}CommercialPositionInputDirectory: {10}{0}CommercialPositionReportDirectory: {11}",
                    Environment.NewLine,
                    FtpServer,
                    FtpUser,
                    FtpPassword,
                    FtpDirectory,
                    FtpArchiveDirectory,
                    LocalBaseDirectory,
                    LastTimeStampFile,
                    CommercialPositionDirectory,
                    CommercialPositionIncomingDirectory,
                    CommercialPositionInputDirectory,
                    CommercialPositionReportDirectory);

                LogInfo(null, info, CurrentClass, currentMethod);

                //// prepare directories for processing

                //// create directories
                EnsureCreateDirectory(LocalBaseDirectory);
                EnsureCreateDirectory(CommercialPositionDirectory);
                EnsureEmptyDirectory(CommercialPositionIncomingDirectory);
                EnsureEmptyDirectory(CommercialPositionInputDirectory);
                EnsureEmptyDirectory(CommercialPositionReportDirectory);
                //// create file
                EnsureCreateFile(LastTimeStampFile);

                ftp = GetFtp(FtpServer, FtpUser, FtpPassword);
                ECSOGMILogManager.GMIlog("Application works", GMI_ECSO_Scenario.Application, true);
            }
            catch (Exception ex)
            {
                LogError(null, "Error Preparing Directory or File for CommercialPositionTask", CurrentClass,
                    currentMethod, ex);
                return false;
            }

            return true;
        }

        private const int BufferSize = 262144;
        private ConcurrentQueue<string> linesRead = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> resultQueue = new ConcurrentQueue<string>();

        //private ConcurrentDictionary<string, USER_ATTRIBUTE> dataDict = new ConcurrentDictionary<string, USER_ATTRIBUTE>();
        private bool fileReadcompleted = false;
        private bool completed = false;

        private readonly object _lockObject = new object();
        private readonly object _fileLockObject = new object();

        public override void DoTask()
        {
            var currentMethod = MethodBase.GetCurrentMethod();

            LogInfo(null, "CommercialPositionTask Begins", CurrentClass, currentMethod);

            if (!Initialize())
            {
                LogError(null, "CommercialPositionTask initialize fail, Task cannot continue", CurrentClass,
                    currentMethod);
                ECSOGMILogManager.GMIlog("CommercialPositionTask initialize fail", GMI_ECSO_Scenario.Application, false);
                return;
            }

            LogInfo(null, "CommercialPositionTask Downloads CommercialPosition File", CurrentClass, currentMethod);

            //// get file listing
            var files = FtpDirectoryListSimple(FtpDirectory);
            //// check for available file to download
            if (files == null || !files.Any())
            {
                LogError(null, "No CommercialPosition File available to download", CurrentClass, currentMethod);
                return;
            }
            //// Get file name
            /// 

            var messages = "";

            foreach (var file in files)
            {
                //var file = files.FirstOrDefault();
                //// validate file name
                if (file == null || string.IsNullOrWhiteSpace(file))
                {
                    LogError(file, "Invalid CommercialPosition File Name", CurrentClass, currentMethod);
                    return;
                }

                var fileParts = file.Split(new char[] {'_', '.'}, StringSplitOptions.RemoveEmptyEntries);

                //if (fileParts.Length != 4)
                //{
                //    LogError(file, "Expect file format \"searchnav_userexport_{datetime}.*\"", CurrentClass, currentMethod);
                //    return;
                //}

                //// build remote and local file name
                //var remoteFile = Path.Combine(FtpDirectory, file);
                var remoteFile = string.Format("/{0}/{1}", FtpDirectory, file);
                var remoteArchiveFile = string.Format("{0}/{1}", FtpArchiveDirectory, file);
                var remoteReportFile = string.Format("{0}/{1}/Report.{2}", FtpDirectory, FtpArchiveDirectory, file);
                var localFile = Path.Combine(CommercialPositionIncomingDirectory, file);

                ////// validate File CreateDate
                //var newCreateDateTime = DateTime.MinValue;
                //var newCreateDateTimeString = fileParts[2];
                //if (string.IsNullOrWhiteSpace(newCreateDateTimeString) ||
                //    !DateTime.TryParseExact(newCreateDateTimeString, CommercialPositionPattern, null, DateTimeStyles.None,
                //        out newCreateDateTime))
                //{
                //    LogError(remoteFile, "Error Getting FTP file CreateDateTime", CurrentClass, currentMethod);
                //    return;
                //}

                //// download file from Ftp Server
                if (!FtpDownload(remoteFile, localFile))
                {
                    LogError(remoteFile, "Error Downloading CommercialPosition File", CurrentClass, currentMethod);
                    return;
                }
                //// validate Zip file
                //if (!ZipFile.IsZipFile(localFile, true))
                //{
                //    LogError(file, "Error Corrupted Zip File", CurrentClass, currentMethod);
                //    return;
                //}
                //// check existence of report location
                if (!Directory.Exists(CommercialPositionReportDirectory))
                {
                    LogError(CommercialPositionReportDirectory, "Error No required directory", CurrentClass,
                        currentMethod);
                    return;
                }

                //// extract zip file
                //using (var zip = ZipFile.Read(localFile))
                //{
                //    zip.ExtractAll(CommercialPositionInputDirectory, ExtractExistingFileAction.OverwriteSilently);
                //}

                //// Get CommercialPosition file name
                //var cpFiles = Directory.GetFiles(CommercialPositionInputDirectory, "*.csv",
                //    SearchOption.TopDirectoryOnly);
                var cpFile = Path.Combine(CommercialPositionInputDirectory, file);

                if (string.IsNullOrWhiteSpace(cpFile))
                {
                    LogError(cpFile, "Invalid CommercialPosition File", CurrentClass, currentMethod);
                    return;
                }

                LogInfo(null, "CommercialPositionTask Reads CommercialPosition and Merging data", CurrentClass,
                    currentMethod);

                var filereadDelay = 600; // This should be configurable
                var reportEveryLines = 1000; // This should be configurable
                var linesInMemThrottle = 2500; // This should be configurable
                var batch = 1000; // This should be configurable

                LogInfo(null, string.Format("filereadDelay: {0}", filereadDelay), CurrentClass, currentMethod);
                LogInfo(null, string.Format("reportEveryLines: {0}", reportEveryLines), CurrentClass, currentMethod);
                ValidateParameterKey("linesInMemThrottle", ref linesInMemThrottle);

                var cpHandler = new CommercialPositionHandler(_createElmDbContext);

                // This line was extracted to be separate method for accessibility of mock object
                var reportLocation = GetReportLocation();

                /*
                 * 
                 
                 string from  = "aaainfrastructure@thomsonreuters.com";
                 var firstOrDefault = dbContext.ECSO_SETTING.FirstOrDefault(r => r.KEY == "cpMailingList");

                 * */

                // Process the file and report
                ProcessFile(filereadDelay, cpFile, reportEveryLines, currentMethod, linesInMemThrottle, batch, cpHandler,
                    reportLocation);


                if (File.Exists(reportLocation))
                {
                    LogInfo(null, string.Format("Uploading file src: {0}, dst: {1}", reportLocation, remoteReportFile),
                        CurrentClass, currentMethod);

                    var newReportName = GetNewFileName(remoteReportFile);
                    FtpUpload(newReportName, reportLocation);

                    var messageLines = File.ReadAllLines(reportLocation);

                    var message = messageLines.Aggregate(string.Empty, (current, m) => string.Format("{0}\n{1}", current, m));

                    messages = string.Format("\n{0}{1}: {2}\n\n", messages, file, message);
                }
                else
                {
                    LogInfo(null, string.Format("Report is NOT generated (no data) for file: [{0}]", file),
                        CurrentClass, currentMethod);

                    messages = string.Format("\n{0}{1}: {2}\n\n", messages, file, string.Empty);
                }

                LogInfo(null, string.Format("Moving file src: {0}, dst: {1}", remoteFile, remoteArchiveFile),
                    CurrentClass, currentMethod);

                var newName = GetNewFileName(string.Format("{0}/{1}", FtpDirectory, remoteArchiveFile));

                newName = newName.Replace(string.Format("{0}/", FtpDirectory), "");
                FtpMove(remoteFile, newName);
            }

            var fromEmailAddress = "aaainfrastructure@thomsonreuters.com";
            var toEmailAddressSetting = _createElmDbContext().ECSO_SETTING.FirstOrDefault(r => r.KEY == "cpMailingList");
            var sendNotificationEmail = toEmailAddressSetting != null && !string.IsNullOrWhiteSpace(toEmailAddressSetting.VALUE);
            var subjectEmail = _createElmDbContext().ECSO_SETTING.FirstOrDefault(r => r.KEY == "cpEmailSubject"); ;

            if (sendNotificationEmail)
            {
                try
                {
                    var env = ELMConfigurationManager.GetELMServiceConfigManager().Environment;
                    var message = messages;
                    var subject = subjectEmail == null
                        ? string.Format("[{0} CommercialPosition Load] Result for {1}", env.ToUpper(), DateTime.Now.ToString("yyyy-MM-dd"))
                        : string.Format("[{0} {1}] Result", env.ToUpper(), subjectEmail);
                    var toEmailAddress = toEmailAddressSetting.VALUE;
                    var distrib = toEmailAddress.Split(new[] { ',', ';' });

                    var distributeList = distrib.ToList();
                    var from = fromEmailAddress;

                    SendNotifyEmail(from, distributeList, subject, message);
                }
                catch (Exception)
                {
                    LogError("CommercialPosition Load", string.Format("Notification Email cannot be sent to {0}", toEmailAddressSetting.VALUE), CurrentClass, currentMethod);
                }

            }
            LogInfo(null, "CommercialPositionTask Ends", CurrentClass, currentMethod);
            ECSOGMILogManager.GMIlog("Application Works", GMI_ECSO_Scenario.Application, true);

        }

        public virtual string GetReportLocation()
        {
            var reportLocation = Path.Combine(CommercialPositionReportDirectory,
                DateTime.Now.ToString("yyyyMMdd_HHmmss.report"));
            return reportLocation;
        }

        // Change private to public virtual to make unit test able to access
        public virtual string GetNewFileName(string remoteArchiveFile)
        {
            string[] list;

            var i = 0;
            var newName = remoteArchiveFile;
            while (true)
            {
                list = FtpDirectoryListSimple(newName);
                if (list == null || list.Length == 0)
                {
                    break;
                }

                newName = string.Format("{0}.{1}", remoteArchiveFile, i);
                i++;
            }
            return newName;
        }

        [ExcludeFromCodeCoverage]
        public virtual bool FtpMove(string src, string dest)
        {
            return ftp.Rename(src, dest);
        }

        [ExcludeFromCodeCoverage]
        public virtual bool FtpUpload(string remote, string local)
        {
            return ftp.Upload(remote, local);
        }

        public void ProcessFile(int filereadDelay, string cpFile, int reportEveryLines, MethodBase currentMethod,
            int linesInMemThrottle, int batch, CommercialPositionHandler cpHandler, string reportLocation)
        {

            lock (_fileLockObject)
            {
                fileReadcompleted = false;
            }

            lock (_lockObject)
            {
                completed = false;
            }

            // This is for testing single-thread for data verification
            ReadFile(filereadDelay, cpFile, reportEveryLines, currentMethod, linesInMemThrottle);
            SaveData(currentMethod, batch, cpHandler);
            ReportResult(reportEveryLines, currentMethod, reportLocation);

            //Parallel.Invoke(
            //    () => // File Reader
            //        ReadFile(filereadDelay, cpFile, reportEveryLines, currentMethod, linesInMemThrottle),
            //    () => // Commercial Position Processor
            //        SaveData(currentMethod, batch, cpHandler),
            //    () => // Report Generator
            //        ReportResult(reportEveryLines, currentMethod, reportLocation));
        }

        /// <summary>
        /// Reporting task
        /// </summary>
        /// <param name="reportEveryLines">number of lines writing to a file each batch</param>
        /// <param name="currentMethod">for logging purpose</param>
        /// <param name="reportLocationIn">the file path to write report to</param>
        public virtual void ReportResult(int reportEveryLines, MethodBase currentMethod, string reportLocationIn)
        {
            var threadName = "Report Generator";
            // Thread.CurrentThread.Name = Thread.CurrentThread.Name;
            var waitCount0 = 0;
            var waitCount1 = 0;

            var reportLocation = reportLocationIn;
            var lineList = new List<string>();

            try
            {

                // Loop 
                while (true)
                {
                    // if resultQueue is empty check if the previous work is completed or not
                    // if complete then stop the loop
                    // if not keep waiting for the previous work to put more data into the result queue
                    if (resultQueue.IsEmpty)
                    {
                        if (completed)
                        {
                            break;
                        }
                        else
                        {
                            LogInfo(null,
                                string.Format("[{0}] {1}: Waiting... ", threadName, waitCount1),
                                CurrentClass, currentMethod);
                            waitCount1++;
                            Thread.Sleep(1000);
                            continue;
                        }
                    }

                    // Once resultQueus has data, just dequeue it
                    // if cannot dequeue, report problem and keep looping 
                    // (this might be because other tasks(if any) may have already dequeue the available data
                    string first = null;
                    if (!resultQueue.TryDequeue(out first))
                    {
                        LogInfo(null, string.Format("[{0}] {1} problem reading ", threadName,
                            waitCount0), CurrentClass, currentMethod);
                        continue;
                    }

                    // Add to a queue to do report
                    lineList.Add(first);

                    // Write to file as a report using first in a batch of reportEveryLines
                    if (resultQueue.Count != 0 && resultQueue.Count%reportEveryLines == 0)
                    {
                        LogInfo(null,
                            string.Format("[{0}]: Writing {1} lines to report ... ", threadName,
                                lineList.Count),
                            CurrentClass, currentMethod);
                        File.AppendAllLines(reportLocation, lineList);
                        lineList.Clear();
                    }
                }

                if (lineList.Any())
                {
                    // Write the rest in the queue
                    LogInfo(null, string.Format("[{0}]: Writing {1} lines to report ... ", threadName,
                        lineList.Count), CurrentClass, currentMethod);
                    File.AppendAllLines(reportLocation, lineList);
                    lineList.Clear();
                }
            }
            catch (Exception ex)
            {
                LogInfo(null, string.Format("[{0}]: Error Occurred during saving report", threadName),
                    CurrentClass, currentMethod);
            }

        }

        /// <summary>
        /// Save data task
        /// </summary>
        /// <param name="currentMethod">for loggin purpose</param>
        /// <param name="batch">number of each processing batch</param>
        /// <param name="cpHandler">handler to call save data</param>
        public void SaveData(MethodBase currentMethod, int batch, CommercialPositionHandler cpHandler)
        {
            var threadName = "Commercial Position Processor";
            // Thread.CurrentThread.Name = threadName;
            var waitCount0 = 0;
            var waitCount1 = 0;

            var linesToProcess = new List<string>();

            var firstLine = true;

            try
            {

                // Loop
                while (true)
                {
                    // if lines in linesRead queue is empty
                    // if the file read is completed then finish the save data
                    // otherwise keep waiting
                    if (linesRead.IsEmpty)
                    {
                        if (fileReadcompleted)
                        {
                            break;
                        }
                        else
                        {
                            LogInfo(null, string.Format("[{0}] {1} Waiting for File Read... ",
                                threadName, waitCount1), CurrentClass, currentMethod);
                            waitCount0++;
                            Thread.Sleep(1000);
                            continue;
                        }
                    }

                    // Once linesRead has data, just dequeue it
                    // if cannot dequeue, report problem and keep looping 
                    // (this might be because other tasks(if any) may have already dequeue the available data
                    string first = null;
                    if (!linesRead.TryDequeue(out first))
                    {
                        LogInfo(null, string.Format("[{0}] {1} problem reading ", threadName,
                            waitCount0), CurrentClass, currentMethod);
                        continue;
                    }

                    if (firstLine)
                    {
                        cpHandler.SetHeader(first);
                        firstLine = false;
                        continue;
                    }
                        
                    // Add to a batch to process
                    linesToProcess.Add(first);

                    // When lines to process is greater than defined batch
                    // save it as a batch and collect the result
                    if (linesToProcess.Count > batch)
                    {
                        var result = cpHandler.Save(linesToProcess);

                        result.ParsingFail.Result.ForEach(
                            s => resultQueue.Enqueue(string.Format("Parsing Fail: {0}", s)));
                        result.ProcessResult.Result.ForEach(
                            r => resultQueue.Enqueue(string.Format("Process: {0}", r.ToString())));
                        linesToProcess.Clear();
                    }
                }

                if (linesToProcess.Any())
                {
                    // Save the rest and collect result
                    var resultx = cpHandler.Save(linesToProcess);

                    resultx.ParsingFail.Result.ForEach(s => resultQueue.Enqueue(string.Format("Parsing Fail: {0}", s)));
                    resultx.ProcessResult.Result.ForEach(
                        r => resultQueue.Enqueue(string.Format("Process: {0}", r.ToString())));
                    linesToProcess.Clear();
                }
                // We are done saving data here
            }
            catch (Exception ex)
            {
                resultQueue.Enqueue(string.Format("Saving Data Failed: {0}, Process Forcely Terminated", ex.Message));
            }
            finally
            {
                lock (_lockObject)
                {
                    completed = true;
                }
            }
        }

        /// <summary>
        /// File Reader task
        /// </summary>
        /// <param name="filereadDelay">delay for each loop to not fill too many lines to the queue</param>
        /// <param name="cpFile">file location of the CSV commercial position file</param>
        /// <param name="reportEveryLines">for throttling</param>
        /// <param name="currentMethod">for logging purpose</param>
        /// <param name="linesInMemThrottle">for throttling</param>
        public virtual void ReadFile(int filereadDelay, string cpFile, int reportEveryLines, MethodBase currentMethod,
            int linesInMemThrottle)
        {
            var threadName = "File Reader";
            // Thread.CurrentThread.Name = threadName;

            var delay = filereadDelay;
            var totalLine = 0;

            try
            {

                //// read file
                using (var fs = File.Open(cpFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var bs = new BufferedStream(fs, BufferSize))
                using (var sr = new StreamReader(bs))
                {
                    string line = null;

                    // Loop read file until no more lines to read
                    // but will do some throttle to make sure not to flush all data at once to the queue
                    while ((line = sr.ReadLine()) != null)
                    {
                        totalLine++;
                        if (totalLine%reportEveryLines == 0)
                        {
                            LogInfo(null, string.Format("Lines Read = {0}, in Mem {1}", totalLine, linesRead.Count),
                                CurrentClass, currentMethod);

                            if ((linesRead.Count > 2*linesInMemThrottle))
                            {
                                while (linesRead.Count > linesInMemThrottle)
                                {
                                    Thread.Sleep(delay);
                                }
                            }
                        }

                        linesRead.Enqueue(line);
                    }

                    LogInfo(null, string.Format("Lines Read = {0}, in Mem {1}", totalLine, linesRead.Count),
                        CurrentClass, currentMethod);

                }
            }
            catch (Exception ex)
            {
                resultQueue.Enqueue(string.Format("Reading File Failed: {0}, Process Forcely Terminated", ex.Message));
            }
            finally
            {
                // Tell other that the file read is completed
                lock (_fileLockObject)
                {
                    fileReadcompleted = true;
                }
            }
        }
        [ExcludeFromCodeCoverage]
        public void SendNotifyEmail(string from, List<string> to, string subject, string message)
        {
            var currentMethod = MethodBase.GetCurrentMethod();

            var email = new AAABusEmailNotification()
            {
                From = from,
                To = to.ToArray(),
                Subject = subject,
                Message = message
            };

            ECSOLogManager.Invoking(null, email, "ELM calls ESB Email Data Transfer", CurrentClass, currentMethod, true);
            const string EMAIL_DATA_TRANSFER_PORT = "Exp_AAABus_SendEmail_1_AAABus_SendEmail_1HttpPort";

            var client = new AAABus_SendEmail_1Client(EMAIL_DATA_TRANSFER_PORT);
            var response = client.sendMail_1(email);

            ECSOLogManager.Reply(null, response, "ESB replies ELM Email Data Transfer", CurrentClass, currentMethod, true);

            // ESB responded with error
            if (response.response == Response.Item1)
            {
                throw new Exception(String.Format(ELMMessage.FAIL_ORCHESTRATION, response.responseCode, response.responseMessage));
            }
        }
    }

}
