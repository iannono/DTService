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
        et_temp,
        flightplan,
        groupincome,
        hubincome,
        hubincome_temp,
        lineincome,
        fltincome

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
                            cmd.CommandText = "delete from pincome where month='" + ImportMonth + "'";
                            cmd.ExecuteNonQuery();

                            InsertIntoTable(table, cmd, filePath);
                        }
                        else if (table == TableName.hubincome)
                        {
                            cmd.CommandText = "delete from hubincome where month='" + ImportMonth + "'";
                            cmd.ExecuteNonQuery();

                            InsertIntoTable(table, cmd, filePath); 
                        }
                        else if (table == TableName.et)
                        { 
                            //et的数据文件，有可能一个文件含有很多天的数据，所以需要根据文件的数据的日期来做判断，对数据库进行更新，而不能直接删除原有的数据
                            //采用一个额外的数据来维护当前文件中所包含的天数，最后根据数组来删除数据库中对应日期的数据，然后再插入新的数据
                            ArrayList dateAry = InsertIntoEt(cmd, filePath);

                            cmd.CommandText = "delete from et where convert(varchar(12), fltdate, 112) in (";
                            foreach (string date in dateAry)
                            {
                                cmd.CommandText += (string)date.Replace("-", "") + " ";
                            }
                            cmd.CommandText += ")";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "insert et select fltdate, sale, localsale, nationalsale, hvpsale, fcsale, wysale, customsale, groupsale, hubsale, directsale," +
                                              "localhub, nationalhub from et_temp";
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
                        throw new Exception(e.Message);
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

            try
            {
                StreamReader sr = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                                                                  System.Text.Encoding.Default);
                //fltincome没有表头，所以不用跳过表头这一行
                if (table != TableName.fltincome)
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
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
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

        private ArrayList InsertIntoEt(SqlCommand cmd, string filePath)
        {
            ArrayList dateAry = new ArrayList();
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
                    if(!dateAry.Contains(row[0].ToString()))
                    {
                        dateAry.Add(row[0].ToString());
                    }
                    commandText.Append(GenerateInsertStr(TableName.et_temp, GenerateValuesFromExcelRow(row)) + "\n");
                    if (count == 5000)
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
            return dateAry;
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
                    valueStr = InsertWithCommon(values, valueStr);
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
                case TableName.fltincome:
                    valueStr = InsertWithFltIncome(values, valueStr);
                    break;
                default:
                    throw new Exception("没有对应的table");
            }
            commandText += valueStr.Substring(0, valueStr.Length - 1) + ");";
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

        //数据表中没有‘月日’这个字段(字段序列3)，但是原始数据里面有，所以需要在生成语句的时候删除
        private string InsertWithFltIncome(string[] values, string valueStr)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 2)
                    continue;
                if (count == 3)
                {
                    valueStr += value.Substring(0, 4);
                    continue;//eg：2013年
                }
                if (count == 4)
                { 
                    valueStr += value.Substring(0, 2); 
                    continue;//eg：05月
                }
                if (count == 5)
                { 
                    valueStr += value.Substring(2, 2); 
                    continue;//eg：第19周
                }
                if (count == 7)
                { 
                    valueStr += "'" + ConvertDayNameToInt(value) + "'"; 
                    continue;//eg：周一
                }
                count++;
            }
            return valueStr; 
        }

        private int ConvertDayNameToInt(string dayName)
        {
            switch (dayName)
            {
                case "周一":
                    return 1;
                case "周二":
                    return 2;
                case "周三":
                    return 3;
                case "周四":
                    return 4;
                case "周五":
                    return 5;
                case "周六":
                    return 6;
                case "周日":
                    return 7;
            }
            return 0;
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
    }
}