﻿using System;
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
                    if (File.Exists(filePath))
                    {
                        success = dataHandler.HandleData(table, filePath);
                        if (success)
                        {
                            results += "<p>" + Enum.GetName(typeof(TableName), table) + ": <span class='label label-success'>成功！</span></p>";
                            try
                            {
                                MoveToHistory(filePath, dir);
                            }
                            catch
                            {
                                throw new Exception(results + "<p class='text-error'>由于无法将导入完成的文件(" + filePath +")移动到history文件夹，导入中止，请检查该文件夹下是否有同名文件，并手动移动该文件!</p>");
                            }
                        }
                        else
                        {
                            results += "<p>" + Enum.GetName(typeof(TableName), table) + ":<span class='label label-important'>失败！</span></p>";
                        }
                    }
                }
            }
            else
            {
                results += "导入目录不存在，请检查是否存在" + Enum.GetName(typeof(TableName), table) + "目录!";
            }
            return results;
        }

        public object GetTableTypeFromTableName(string tableName)
        {
            return Enum.Parse(typeof(TableName), tableName);
        }

        /// <summary>
        /// 已经导入的数据文档移动到history文件夹
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="dirPath"></param>
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
            switch (table)
            {
                case TableName.pincome:
                    return filePath + @"\pincome\";
                case TableName.cincome:
                    return filePath + @"\cincome\";
                case TableName.cargoincome:
                    return filePath + @"\cargoincome\";
                case TableName.et:
                    return filePath + @"\et\";
                case TableName.flightplan:
                    return filePath + @"\flightplan\";
                case TableName.groupincome:
                    return filePath + @"\groupincome\";
                case TableName.hubincome:
                    return filePath + @"\hubincome\";
                case TableName.lineincome:
                    return filePath + @"\lineincome\";
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