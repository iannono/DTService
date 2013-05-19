using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data;
using System.IO;
using LinqToExcel;

namespace DTService.Handlers
{
    public enum HandleType
    {
        Insert,
        Update,
        UpdateAndInsert
    }

    public enum TableName
    {
        pincome,
        pincome_temp,
        cincome,
        cargoincome,
        et,
        flightplan,
        groupincome,
        hubincome,
        hubincome_temp,
        lineincome

    }

    public class DataHandler
    {
        string _connStr = ConfigurationManager.ConnectionStrings["omsConnectionString"].ToString();

        public bool HandleData(TableName table, string filePath)
        {
            var success = true;
            using (SqlConnection conn = new SqlConnection(_connStr))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = conn;
                    cmd.Transaction = trans;
                    var ImportMonth = FilterDateFromFilePath(filePath);
                    try
                    {
                        if (table == TableName.pincome)
                        {
                            table = TableName.pincome_temp;
                            InsertIntoTable(table, cmd, filePath);
                            //选出不在当前文件中的数据，写入临时表
                            //删除数据库中和录入文件同一个月的数据
                            //将临时表的数据写入到数据表，这么做主要是为了解决新下载的文件可能不包含原有的部分数据
                            cmd.CommandText = "insert pincome_temp " +
                                              "select month, fltdate, fltno, khcode, khname, xscode, xsname, sfcode, sfname, agtname, agtcode, agtcitycode," +
                                              "agtcityname, line, lineflag, segment, orgncity, destcity, segtype, cls, seattype, linecode, printdate, clsflag," +
                                              "passenger, income, khincome, extrafee, oil, standardfee from pincome where month='" + ImportMonth + "'" + 
                                              " except " +
                                              "select month, fltdate, fltno, khcode, khname, xscode, xsname, sfcode, sfname, agtname, agtcode, agtcitycode," +
                                              "agtcityname, line, lineflag, segment, orgncity, destcity, segtype, cls, seattype, linecode, printdate, clsflag," +
                                              "passenger, income, khincome, extrafee, oil, standardfee from pincome_temp";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "delete from pincome where month='" + ImportMonth + "'";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "insert pincome " +
                                              "select month, fltdate, fltno, khcode, khname, xscode, xsname, sfcode, sfname, agtname, agtcode, agtcitycode," +
                                              "agtcityname, line, lineflag, segment, orgncity, destcity, segtype, cls, seattype, linecode, printdate, clsflag," +
                                              "passenger, income, khincome, extrafee, oil, standardfee from pincome_temp;";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "delete pincome_temp";
                            cmd.ExecuteNonQuery();
                        }
                        else if (table == TableName.hubincome)
                        {
                            table = TableName.hubincome_temp;
                            InsertIntoTable(table, cmd, filePath);
                            cmd.CommandText = "insert hubincome_temp " +
                                              "select month, fltdate, fltno, linecode, khcode, khname, xscode, xsname, sfcode, sfname, agtcode, agtname," +
                                              "agtcitycode, agtcityname, line, lineflag, hub, cls, seattype, orgncity, destcity, segment, segtype, printdate, passenger," +
                                              "income, extrafee, standardfee from hubincome where month='" + ImportMonth + "'" + 
                                              " except " +
                                              "select month, fltdate, fltno, linecode, khcode, khname, xscode, xsname, sfcode, sfname, agtcode, agtname," +
                                              "agtcitycode, agtcityname, line, lineflag, hub, cls, seattype, orgncity, destcity, segment, segtype, printdate, passenger," +
                                              "income, extrafee, standardfee from hubincome_temp";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "delete from hubincome where month='" + ImportMonth + "'";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "insert hubincome " +
                                              "select month, fltdate, fltno, linecode, khcode, khname, xscode, xsname, sfcode, sfname, agtcode, agtname," +
                                              "agtcitycode, agtcityname, line, lineflag, hub, cls, seattype, orgncity, destcity, segment, segtype, printdate, passenger," +
                                              "income, extrafee, standardfee from hubincome_temp";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "delete hubincome_temp";
                            cmd.ExecuteNonQuery();

                        }
                        else
                        {
                            InsertIntoTable(table, cmd, filePath);
                        }
                    }
                    catch (Exception e)
                    {
                        var err = e.Message;
                        //to-do
                        //add error log
                        success = false;
                        trans.Rollback();
                    }
                    finally
                    {
                        if (success)
                            trans.Commit();
                        cmd.Dispose();
                    }
                }
                return success;
            }
        }

        //针对需要更新数据的表，需要了解当前的文件是针对几月的数据
        //这样才能从数据库中筛选出正确的数据，并进行替换
        //所以需要文件的名称的头6个字母标明当前的数据所属的月份
        private string FilterDateFromFilePath(string filePath)
        {
            var ImportDate = filePath.Substring(0, 6);
            return ImportDate; 
        }

        private void InsertIntoTable(TableName table, SqlCommand cmd, string filePath)
        {

            var start = filePath.LastIndexOf('.') + 1;
            var end = filePath.Length - filePath.LastIndexOf('.') - 1;
            var fileType = filePath.Substring(start, end).ToLower();

            if(fileType == "csv" || fileType == "xls" || fileType == "xlsx")
            {
                InsertIntoTableWithExcel(table, cmd, filePath);
            }
            else if(fileType == "txt")
            {
                InsertIntoTableWithTxt(table, cmd, filePath);
            }
        }

        private void InsertIntoTableWithTxt(TableName table, SqlCommand cmd, string filePath)
        {
            StringBuilder commandText = new StringBuilder();
            StreamReader sr = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                                                              System.Text.Encoding.Default);
            sr.ReadLine();
            string strTemp = sr.ReadLine();

            string[] splits = null;
            int count = 0;
            while (strTemp != null)
            {
                count++;
                splits = strTemp.Split('\t');
                commandText.Append(GenerateInsertStr(table, splits) + "\n");
                if (count == 5000)
                {
                    cmd.CommandText = commandText.ToString();
                    cmd.ExecuteNonQuery();
                    count = 0;
                    commandText.Clear();
                }
                strTemp = sr.ReadLine();
            }

            cmd.CommandText = commandText.ToString();
            cmd.ExecuteNonQuery();
            sr.Close();
        }
        private void InsertIntoTableWithExcel(TableName table, SqlCommand cmd, string filePath)
        {
            StringBuilder commandText = new StringBuilder();
            try
            {
                var excel = new ExcelQueryFactory(filePath); 

                var rows = from v in excel.Worksheet()
                           select v;

                var count = 0;
                foreach (var row in rows)
                {
                    count++;
                    commandText.Append(GenerateInsertStr(table, GenerateValuesFromExcelRow(row)) + "\n");
                    if(count == 5000)
                    {
                        cmd.CommandText = commandText.ToString();
                        cmd.ExecuteNonQuery();
                        count = 0;
                        commandText.Clear();
                    }
                }
                cmd.CommandText = commandText.ToString();
                cmd.ExecuteNonQuery(); 
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        //将excel表中的每一行数据转换成对应的字符串数组
        private string[] GenerateValuesFromExcelRow(LinqToExcel.Row row)
        { 
            string[] values = new String[row.ToArray().Length];
            var count = 0;
            foreach (var value in row.ToArray())
            { 
                values[count] = value.ToString();
                count++;
            }
            return values;
        }

        private string GetHandleCommandText(TableName table, HandleType handleType, string[] values, SqlCommand cmd)
        {
            var commandText = "";
            switch (handleType)
            {
                case HandleType.Insert:
                    commandText = GenerateInsertStr(table, values);
                    break;
                case HandleType.UpdateAndInsert:
                    commandText = GenerateUpdateAndInsertStr(table, values, cmd);
                    break;
            }
            return commandText;
        }


        //针对不同的数据表需要转换不同的数据格式以及更新不同的字段
        private string GenerateInsertStr(TableName table, string[] values)
        {
            var commandText = "insert into " + Enum.GetName(typeof(TableName), table) + " (" + FilterEscape(ConfigurationManager.AppSettings[Enum.GetName(typeof(TableName), table)].ToString()) + ") ";
            var valueStr = " values(";
            switch (table)
            {
                case TableName.pincome:
                    valueStr = InsertWithPincome(values, valueStr);
                    break;
                case TableName.pincome_temp:
                    valueStr = InsertWithPincome(values, valueStr);
                    break;
                case TableName.cincome:
                    valueStr = InsertWithPincome(values, valueStr);
                    break;
                case TableName.et:
                    valueStr = InsertWithPincome(values, valueStr);
                    break;
                case TableName.flightplan:
                    valueStr = InsertWithFlightPlan(values, valueStr);
                    break;
                case TableName.groupincome:
                    valueStr = InsertWithCommon(values, valueStr);
                    break;
                case TableName.hubincome:
                    valueStr = InsertWithHubIncome(values, valueStr);
                    break;
                case TableName.hubincome_temp:
                    valueStr = InsertWithHubIncome(values, valueStr);
                    break;
                case TableName.lineincome:
                    valueStr = InsertWithCommon(values, valueStr);
                    break;
                default:
                    throw new Exception("没有对应的table");
            }
            commandText += valueStr.Substring(0, valueStr.Length - 1) + ");";
            return commandText;
        }

        private string GenerateSelectIntoStrWithTable(TableName table)
        {
            var commandText = "";
            switch (table)
            {
                case TableName.pincome:
                    commandText = "select * into pincome_temp from pincome where month='201212'";
                    break;
            }
            return commandText;
        }

        //下面是各个数据表的具体操作
        //pincome
        private string InsertWithPincome(string[] values, string valueStr)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 0)
                {
                    //去除日期中的‘，’,根据数据库的设置，只取前6位
                    valueStr += "'" + value.Replace(",", "").Substring(0, 6) + "',";
                }
                else if( count == 1 || count == 22)
                {
                    valueStr += "'" + value.Replace(",", "").Substring(0, 8) + "',"; 
                }
                else
                {
                    //去除日期和金额中的','
                    valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
                }
                count++;
            }
            return valueStr;
        }
        //hubincome
        private string InsertWithHubIncome(string[] values, string valueStr)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 0)
                {
                    //去除日期中的‘，’,根据数据库的设置，只取前6位
                    valueStr += "'" + value.Replace(",", "").Substring(0, 6) + "',";
                }
                else if (count == 1 || count == 23)
                {
                    valueStr += "'" + value.Replace(",", "").Substring(0, 8) + "',";
                }
                else
                {
                    //去除日期和金额中的','
                    valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
                }
                count++;
            }
            return valueStr;
        }
        //flightplan
        private string InsertWithFlightPlan(string[] values, string valueStr)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 3 || count == 4 || count == 5)
                {
                    //去除日期中的‘，’,根据数据库的设置，只取前6位
                    valueStr += value + ",";
                }
                else
                {
                    //去除日期和金额中的','
                    valueStr += "'" + value + "',";
                }
                count++;
            }
            return valueStr;
        }

        //通用的数据表转化，对于没有特殊字段的表可以调用该方法
        private string InsertWithCommon(string[] values, string valueStr)
        {
            foreach (string value in values)
            {
                valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
            }
            return valueStr;
        }


        //private helper
        private string FilterEscape(string value)
        {
            string filter = @"[\t\n\r\s]";
            value = System.Text.RegularExpressions.Regex.Replace(value, filter, "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return value;
        }

        private string GenerateUpdateStr(TableName table, string[] values)
        {
            var commandText = "update " + Enum.GetName(typeof(TableName), table) + " set ";
            var columnKeys = ConfigurationManager.AppSettings[Enum.GetName(typeof(TableName), table)].ToString().Split(',');
            commandText = UpdateWithPincome(values, commandText, columnKeys);
            return commandText;
        }

        //以下方法暂时没有作用，未来可以视情况进行删除
        private string UpdateWithPincome(string[] values, string commandText, string[] columnKeys)
        {
            var count = 0;
            foreach (string columnKey in columnKeys)
            {

                if (count == 0 || count == 1)
                {
                    //去除日期中的‘，’,根据数据库的设置，只取前6位
                    commandText += FilterEscape(columnKey) + "='" + values[count].Replace(",", "").Substring(0, 6) + "',";
                }
                else if (count == 22)
                {
                    commandText += FilterEscape(columnKey) + "='" + values[count].Replace(",", "").Substring(0, 8) + "',";
                }
                else
                {
                    //去除日期和金额中的','
                    commandText += FilterEscape(columnKey) + "='" + values[count].Replace(",", "").Replace("'", "''") + "',";
                }
                count++;
            }

            commandText = commandText.Substring(0, commandText.Length - 1);
            commandText += string.Format(" where month='{0}' and fltdate='{1}' and fltno='{2}' and khcode='{3}' and xscode='{4}' and sfcode='{5}' and " +
                                      "agtcode='{6}' and line='{7}' and lineflag='{8}' and segment='{9}' and segtype='{10}' and cls='{11}' and seattype='{12}' and linecode='{13}' and clsflag='{14}'; ",
                                      values[0].Replace(",", "").Substring(0, 6), values[1].Replace(",", "").Substring(0, 6), values[2], values[3], values[5], values[7], values[10], values[13], values[14],
                                      values[15], values[18], values[19], values[20], values[21], values[22].Replace(",", "").Substring(0, 8));
            return commandText;
        }
 
        private string GenerateSelectStrWithTable(TableName table, string[] values)
        {
            var selectStr = "";
            switch (table)
            {
                case TableName.pincome:
                    selectStr = string.Format("select month from " +
                                        Enum.GetName(typeof(TableName), table) +
                                        " where month='{0}' and fltdate='{1}' and fltno='{2}' and khcode='{3}' and xscode='{4}' and sfcode='{5}' and " +
                                        "agtcode='{6}' and line='{7}' and lineflag='{8}' and segment='{9}' and segtype='{10}' and cls='{11}' and seattype='{12}' and linecode='{13}' and clsflag='{14}' ",
                                        values[0].Replace(",", "").Substring(0, 6), values[1].Replace(",", "").Substring(0, 6), values[2], values[3], values[5], values[7], values[10], values[13], values[14],
                                        values[15], values[18], values[19], values[20], values[21], values[23]);
                    break;
            }
            return selectStr;
        }
 
        private string GenerateUpdateAndInsertStr(TableName table, string[] values, SqlCommand cmd)
        {
            var commandText = "";
            var count = 0;
            //select month, fltdate, fltno, khcode, xscode, sfcode, agtcode, line, lineflag segment, segtype, cls, seattype, linecode, clsflag 
            cmd.CommandText = GenerateSelectIntoStrWithTable(table);
            count = cmd.ExecuteNonQuery();

            if (count == -1)
            {
                throw new Exception("数据插入失败!");
            }

            cmd.CommandText = "";
            return commandText;
        }
    }
}