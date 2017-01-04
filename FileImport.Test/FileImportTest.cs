using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Reflection;

namespace FileImport.Test
{
    [TestClass]
    public class FileImportTest
    {
        [TestMethod]
        public void Import()
        {
            var path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var fileName = Path.Combine(path, "data.txt");
            if (File.Exists(fileName))
            {
                var importor = new Importor();
                var result = importor.Import(fileName);
                Assert.IsNotNull(result);
            }
        }
    }
}
