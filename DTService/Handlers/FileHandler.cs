using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Data.SqlClient;
using System.Configuration;
using DTService.Handlers;
using LinqToExcel;

namespace DTService.Handlers
{
    public class FileHandler
    {
        private DataHandler dataHandler = new DataHandler();
        string _connStr = ConfigurationManager.ConnectionStrings["omsConnectionString"].ToString();

        public string ImportFile(TableName table)
        {
            var dir = GetDirPath(table);
            var success = false;
            var results = "";
            if (Directory.Exists(dir))
            {
                var filePaths = Directory.GetFiles(dir);
                results += GenerateResults(filePaths.Length);
                foreach (string filePath in filePaths)
                {
                    if (table == TableName.fltincome && !IsFltIncomeFile(filePath))
                        continue;

                    if (File.Exists(filePath))
                    {
                        try
                        {
                            success = dataHandler.HandleData(table, filePath);
                            if (success)
                            {
                                results += "<p>" + Enum.GetName(typeof(TableName), table) + "(文件路径：" + filePath + "): <span class='label label-success'>成功！</span></p>";
                                try
                                {
                                    MoveToHistory(filePath, dir);
                                }
                                catch
                                {
                                    throw new Exception(results + "<p class='text-error'>无法将导入完成的文件(" + filePath + ")移动到history文件夹，请检查该文件是否已经打开，或者该文件夹下是否有同名文件，如果有，请手动移动该文件!</p>");
                                }
                            }
                            else
                            {
                                results += "<p>" + Enum.GetName(typeof(TableName), table) + ":<span class='label label-important'>失败！</span></p>";
                            }
                        }
                        catch(Exception e)
                        {
                            results = e.Message;
                        }
                    }
                }
            }
            else
            {
                results += "导入目录不存在，请检查是否存在" + Enum.GetName(typeof(TableName), table) + "表所对应的目录!";
            }
            return results;
        }

        public bool IsFltIncomeFile(string filePath)
        {
            if (filePath.Contains("SEGMENT"))
                return true;
            return false;
        }

        public object GetTableTypeFromTableName(string tableName)
        {
            return Enum.Parse(typeof(TableName), tableName);
        }

        /// <summary>
        /// 已经导入的数据文档移动到history文件夹
        /// </summary>
        /// <param name="filePath">当前需要移动的文件的路径</param>
        /// <param name="dirPath">需要移动到的目录</param>
        private void MoveToHistory(string filePath, string dirPath)
        {
            if (File.Exists(filePath))
            {
                var fileInfo = new FileInfo(filePath);
                File.Move(filePath, CreateDirectory(dirPath + @"\history\") + fileInfo.Name);
            }
        }

        private string GetDirPath(TableName table)
        {
            string filePath = ConfigurationManager.AppSettings["FilePath"].ToString();
            string ftpPath = ConfigurationManager.AppSettings["FtpPath"].ToString();
            switch (table)
            {
                case TableName.pincome:
                    return filePath + @"\客运销售收入贡献\";
                case TableName.cincome:
                    return filePath + @"\常客销售收入\";
                case TableName.cargoincome:
                    return filePath + @"\货邮收入\";
                case TableName.et:
                    return filePath + @"\et\";
                case TableName.flightplan:
                    return filePath + @"\航班计划销售贡献指标\";
                case TableName.groupincome:
                    return filePath + @"\大客户销售收入\";
                case TableName.hubincome:
                    return filePath + @"\枢纽中转销售收入\";
                case TableName.lineincome:
                    return filePath + @"\BO航线座公里收入汇总\";
                case TableName.fltincome:
                    return ftpPath;
            }
            return filePath;
        }

        private string CreateDirectory(string dirPath)
        {
            if (!Directory.Exists(dirPath))
            {
                Directory.CreateDirectory(dirPath);
            }
            return dirPath;
        }

        private string GenerateResults(int fileLength)
        {
            var results = "";
            results += "<h4>导入结果</h4>";
            if (fileLength == 0)
            {
                results += "<p>目录下无导入文件，请检查文件目录!</p>";
            }
            else
            {
                results += "<p>本次导入共需导入：" + fileLength + "个文件，结果如下:</p>";
            }
            return results;
        }
    }
}