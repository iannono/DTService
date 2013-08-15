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
        fltincome,
        sfincome

    } 

    public class DataHandler
    {
        string _connStr = ConfigurationManager.ConnectionStrings["omsConnectionString"].ToString();

        public bool HandleData(TableName table, string filePath, string type)
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
                    var ImportMonth = FilterDateFromFilePath(filePath, "month");
                    type = type.ToUpper();
                    try
                    {
                        if (table == TableName.pincome)
                        { 
                            cmd.CommandText = "delete from pincome where month='" + ImportMonth + "' and company='" + type + "'";
                            cmd.ExecuteNonQuery();

                            InsertIntoTable(table, cmd, filePath, type);
                        }
                        else if (table == TableName.hubincome)
                        {
                            cmd.CommandText = "delete from hubincome where month='" + ImportMonth + "' and khcode='" + type + "'";
                            cmd.ExecuteNonQuery();

                            InsertIntoTable(table, cmd, filePath, type); 
                        }
                        else if (table == TableName.et)
                        { 
                            //et的数据文件，有可能一个文件含有很多天的数据，所以需要根据文件的数据的日期来做判断
                            //对数据库进行更新，而不能直接删除原有的数据
                            //采用一个额外的数据来维护当前文件中所包含的天数
                            //最后根据数组来删除数据库中对应日期的数据，然后再插入新的数据
                            ArrayList dateAry = InsertIntoEt(cmd, filePath, type);

                            cmd.CommandText = "delete from et where convert(varchar(12), fltdate, 112) in (";
                            foreach (string date in dateAry)
                            {
                                cmd.CommandText += "'" + Convert.ToDateTime(date).ToString("yyyyMMdd") + "',";
                            }
                            cmd.CommandText = cmd.CommandText.Substring(0, cmd.CommandText.Length - 1) + ")" + " and company='" + type + "'";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "insert et select fltdate, sale, localsale, nationalsale, hvpsale, fcsale, wysale, customsale, groupsale, hubsale, directsale," +
                                              "localhub, nationalhub, company from et_temp";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "delete from et_temp";
                            cmd.ExecuteNonQuery();
                        }
                        else if (table == TableName.fltincome)
                        {
                            //对于fltincome表，插入数据后
                            //还需要从表中选择部分数据，插入其他的表中
                            InsertIntoFltIncome(cmd, filePath); 
                        }
                        else if (table == TableName.cargoincome)
                        {
                            InsertIntoCargoIncome(cmd, filePath, type);
                        }
                        else if (table == TableName.cincome)
                        {
                            cmd.CommandText = "delete from cincome where month='" + ImportMonth + "' and corporation='" + type + "'";
                            cmd.ExecuteNonQuery();

                            InsertIntoTable(table, cmd, filePath, type); 
                        }
                        else
                        {
                            InsertIntoTable(table, cmd, filePath, type);
                        }
                    }
                    catch (Exception e)
                    {
                        //to-do
                        //add error log
                        success = false;
                        trans.Rollback();
                        throw new Exception("<p class='text-error'>" + e.Message + "</p>");
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
        private string FilterDateFromFilePath(string filePath, string type)
        {
            var ImportDate = "";
            var fileInfo = new FileInfo(filePath);
            var fileName = fileInfo.Name;
            if (File.Exists(filePath))
            {
                if (type == "month")
                {
                    ImportDate = fileName.Substring(0, 6);
                }
                else if (type == "day")
                {
                    ImportDate = fileName.Substring(0, 8);
                }
                else if (type == "day_from_end")
                {
                    ImportDate = fileName.Substring(fileName.LastIndexOf('.') - 8, 8);
                }
            }
            return ImportDate; 
        }
        private string FilterCorporationFromFilePath(string filePath)
        {
            var corporation = "";

            var fileInfo = new FileInfo(filePath);
            var fileName = fileInfo.Name;
            if (File.Exists(filePath))
            {
                corporation = fileName.Substring(0, 3);
            }
            return corporation;
        }

        private void InsertIntoTable(TableName table, SqlCommand cmd, string filePath, string type)
        {

            var start = filePath.LastIndexOf('.') + 1;
            var end = filePath.Length - filePath.LastIndexOf('.') - 1;
            var fileType = filePath.Substring(start, end).ToLower();

            if(fileType == "csv" || fileType == "xls" || fileType == "xlsx")
            {
                InsertIntoTableWithExcel(table, cmd, filePath, type);
            }
            else if(fileType == "txt")
            {
                InsertIntoTableWithTxt(table, cmd, filePath, type);
            }
        }

        private void InsertIntoTableWithTxt(TableName table, SqlCommand cmd, string filePath, string type)
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
                    splits = strTemp.Split('\t');
                    if (count == 0 && (table == TableName.pincome || table == TableName.hubincome))
                    { 
                        //针对pincome,hubincome等表进行数据确认，确定导入的数据和文件名中包含的月份是一样的
                        //目前的检查只是针对第一条进行判断
                        var importMonth = FilterDateFromFilePath(filePath, "month");
                        if (!CheckImportDataWithMonth(table, importMonth, splits))
                            throw new Exception("<p class='text-error'>文件" + filePath + "的名称与内容中数据的所属月份不一致，该文件的导入停止，请检查文件后，再进行导入</p>");

                    }
                    commandText.Append(GenerateInsertStr(table, splits,"",type) + "\n");
                    if (count == 100)
                    {
                        cmd.CommandText = commandText.ToString();
                        cmd.ExecuteNonQuery();
                        count = 0;
                        commandText.Clear();
                    }
                    strTemp = sr.ReadLine();
                    count++;
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

        //检查插入的数据和文件名前面的月份是否一致
        private bool CheckImportDataWithMonth(TableName table, string importMonth, string[] values)
        {
            switch (table)
            { 
                case TableName.pincome:
                    return (importMonth == values[0].Replace(",", "").Substring(0, 6));
                case TableName.hubincome:
                    return (importMonth == values[0].Replace(",", "").Substring(0, 6));
            }
            return false;
        }

        private void InsertIntoTableWithExcel(TableName table, SqlCommand cmd, string filePath, string type)
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
                    if (count == 0 && (table == TableName.pincome || table == TableName.hubincome))
                    {
                        //针对pincome,hubincome等表进行数据确认，确定导入的数据和文件名中包含的月份是一样的
                        var importMonth = FilterDateFromFilePath(filePath, "month");
                        if (!CheckImportDataWithMonth(table, importMonth, GenerateValuesFromExcelRow(row)))
                            throw new Exception("<p class='text-error'>文件" + filePath + "的名称与内容中数据的所属月份不一致，该文件的导入停止，请检查文件后，再进行导入</p>");
                    }
                    commandText.Append(GenerateInsertStr(table, GenerateValuesFromExcelRow((LinqToExcel.Row)row),"", type) + "\n");
                    if(count == 5000)
                    {
                        cmd.CommandText = commandText.ToString();
                        cmd.ExecuteNonQuery();
                        count = 0;
                        commandText.Clear();
                    }
                    count++;
                }
                cmd.CommandText = commandText.ToString();
                cmd.ExecuteNonQuery(); 
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        private ArrayList InsertIntoEt(SqlCommand cmd, string filePath, string type)
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
                    commandText.Append(GenerateInsertStr(TableName.et_temp, GenerateValuesFromExcelRow(row),"", type) + "\n");
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

        private void InsertIntoFltIncome(SqlCommand cmd, string filePath)
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder commandTextWithSfincome = new StringBuilder();
            try
            {
                StreamReader sr = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                                                                  System.Text.Encoding.Default);

                string strTemp = sr.ReadLine(); 
                string[] splits = null; 
                var count = 0;


                //导入数居前，需要先删除该日期下的已有的数据
                var dateTime = FilterDateFromFilePath(filePath, "day_from_end");
                var corporation = FilterCorporationFromFilePath(filePath);
                cmd.CommandText = "delete from fltincome where convert(varchar(12), fltdate, 112)='" + dateTime + "' and corporation = '" + corporation + "'";
                cmd.ExecuteNonQuery();

                //导入数据前，如果是WUH公司的数据，则需要删除对应日期下的sfincome表
                if (corporation == "WUH")
                {
                    cmd.CommandText = "delete from sfincome where convert(varchar(12), fltdate, 112)='" + dateTime + "'";
                    cmd.ExecuteNonQuery();
                }

                while (strTemp != null)
                {
                    count++;
                    splits = strTemp.Split(',');

                    commandText.Append(GenerateInsertStr(TableName.fltincome, splits, filePath) + "\n");

                    if (count % 1000 == 0)
                    {
                        cmd.CommandText = commandText.ToString();
                        cmd.ExecuteNonQuery();
                        commandText.Clear();
                    }


                    //-------以下是生成从fltincome中抽取数据，插入到sfincome表中的语句-------------//
                    //如果承运人是CZ，并且航线中含有（WUH、YIH、ENH、XFN）等，并且（除共享标志为1且执行单位为空的）,并且是WUH公司的才需要录入到Sfincome;
                    if (FilterLine(splits[8].ToString(), splits[16].ToString(), splits[36].ToString(), splits[39].ToString()) && corporation == "WUH")
                        commandTextWithSfincome.Append(GenerateInsertStr(TableName.sfincome, splits) + "\n");

                    strTemp = sr.ReadLine();
                }


                cmd.CommandText = commandText.ToString();
                cmd.ExecuteNonQuery();

                if (commandTextWithSfincome.ToString() != "")
                { 
                    cmd.CommandText = commandTextWithSfincome.ToString();
                    cmd.ExecuteNonQuery();
                }
                sr.Close();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        //因为CargoIncome需要根据文件名来读取录入的日期，所以单独使用一个方法
        private void InsertIntoCargoIncome(SqlCommand cmd, string filePath, string type)
        { 
            ArrayList dateAry = new ArrayList();
            StringBuilder commandText = new StringBuilder();
            try
            {
                var excel = new ExcelQueryFactory(filePath);

                var rows = from v in excel.Worksheet("sheet0")
                           select v; 

                var dateTime = FilterDateFromFilePath(filePath, "day");
                var count = 0;

                //导入前需先删除同一天的数据
                cmd.CommandText = "delete from cargoincome where convert(varchar(12), fltdate, 112)='" + dateTime + "' and company='" + type + "'";
                cmd.ExecuteNonQuery();

                foreach (var row in rows)
                {
                    count++;
                    if (count == 1 || count == rows.Count())//跳过第一行的日期以及最后一行的合计
                        continue;

                    commandText.Append(InsertWithCargoIncome(TableName.cargoincome,GenerateValuesFromExcelRow(row), dateTime, type) + "\n");

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
        private string[] GenerateValuesFromExcelRowNoHeader(LinqToExcel.RowNoHeader row)
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


        //针对不同的数据表需要转换不同的数据格式以及更新不同的字段
        //生成每条插入数据的sql语句
        private string GenerateInsertStr(TableName table, string[] values, string filePath = "", string type = "")
        {
            var commandText = "insert into " + Enum.GetName(typeof(TableName), table) + " (" + FilterEscape(ConfigurationManager.AppSettings[Enum.GetName(typeof(TableName), table)].ToString()) + ") ";
            var valueStr = " values(";
            switch (table)
            {
                case TableName.pincome:
                    valueStr = InsertWithPincome(values, valueStr, type);
                    break;
                case TableName.pincome_temp:
                    valueStr = InsertWithPincome(values, valueStr, type);
                    break;
                case TableName.cincome:
                    valueStr = InsertWithCIncome(values, valueStr, type);
                    break;
                case TableName.et:
                    valueStr = InsertWithCommon(values, valueStr, table, type);
                    break;
                case TableName.et_temp:
                    valueStr = InsertWithCommon(values, valueStr, table, type);
                    break;
                case TableName.flightplan:
                    valueStr = InsertWithFlightPlan(values, valueStr, type);
                    break;
                case TableName.groupincome:
                    valueStr = InsertWithGroupIncome(values, valueStr);
                    break;
                case TableName.hubincome:
                    valueStr = InsertWithHubIncome(values, valueStr);
                    break;
                case TableName.hubincome_temp:
                    valueStr = InsertWithHubIncome(values, valueStr);
                    break;
                case TableName.lineincome:
                    valueStr = InsertWithCommon(values, valueStr, table, type);
                    break;
                case TableName.fltincome:
                    valueStr = InsertWithFltIncome(values, valueStr, filePath);
                    break;
                case TableName.sfincome:
                    valueStr = InsertWithSfIncome(values, valueStr);
                    break;
                default:
                    throw new Exception("没有对应的table");
            }
            commandText += valueStr.Substring(0, valueStr.Length - 1) + ");";
            return commandText;
        }

        //下面是各个数据表的具体操作
        //pincome
        private string InsertWithPincome(string[] values, string valueStr, string type)
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
                else if (count == 10)
                {
                    valueStr += "'" + DeleteAgtCodeWithZero(value) + "',";
                }
                else
                {
                    //去除日期和金额中的','
                    valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
                }
                count++;
            }
            valueStr += AddCompany(TableName.pincome, type) + ",";
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
                else if (count == 10)
                {
                    valueStr += "'" + DeleteAgtCodeWithZero(value) + "',";
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

        private string InsertWithCIncome(string[] values, string valueStr, string type)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 1)
                {
                    valueStr += "'" + DeleteAgtCodeWithZero(value) + "',";
                }
                else
                {
                    valueStr += "'" + value.Replace(",", "").Replace("'", "''").Replace(@"""","") + "',";
                }
                count++;
            }
            valueStr += AddCompany(TableName.cincome, type) + ",";
            return valueStr;
        }

        private string InsertWithGroupIncome(string[] values, string valueStr)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 2 || count == 9)
                {
                    valueStr += "'" + DeleteAgtCodeWithZero(value) + "',";
                }
                else
                {
                    valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
                }
                count++;
            }
            return valueStr;
        }
        //flightplan
        private string InsertWithFlightPlan(string[] values, string valueStr, string type)
        {
            var count = 0;
            foreach (string value in values)
            {
                if (count == 3 || count == 4)
                {
                    //int型
                    valueStr += value + ",";
                }
                else if (count == 5)
                {
                    valueStr += Math.Round(Convert.ToDecimal(value), MidpointRounding.AwayFromZero) + ",";
                }
                else
                {
                    //去除日期和金额中的','
                    valueStr += "'" + value + "',";
                }
                count++;
            }
            valueStr += AddCompany(TableName.flightplan, type) + ",";
            return valueStr;
        }

        //数据表中没有‘月日’这个字段(字段序列3)，但是原始数据里面有，所以需要在生成语句的时候删除
        private string InsertWithFltIncome(string[] values, string valueStr, string filePath)
        {
            var count = 0;
            foreach (string value in values)
            { 
                count++;

                if (count == 2)
                {
                    //这个字段根据文件类型的不同，读取的内容会有差异（csv：2013-05；xlsx：2013/05/01 00:00:00）
                    valueStr += "'" + value.Replace("-", "") + "',";
                    continue;//eg：201305
                }
                if (count == 3)
                    continue;
                if (count == 4)
                {
                    valueStr += "'" + value.Substring(0, 4) + "',";
                    continue;//eg：2013年
                }
                if (count == 5)
                { 
                    valueStr += "'" + value.Substring(0, 2) + "',"; 
                    continue;//eg：05月
                }
                if (count == 6)
                { 
                    valueStr += "'" + value.Substring(1, 2) + "',"; 
                    continue;//eg：第19周
                }
                if (count == 8)
                { 
                    valueStr += "'" + ConvertDayNameToInt(value) + "',"; 
                    continue;//eg：周一
                }
                if(count == 10)
                {
                    valueStr += "'" + value + "','" + GenerateCarriernameunionFromCarriername(value) + "',";
                    continue;
                }
                if (count == 51 || count == 56 || count == 57 || count == 58 || count == 59 || count == 65 || count == 69 || count == 70)
                {
                    if (value.Length >= 11)
                    {
                        valueStr += "'" + value.Substring(0, 11) + "',";
                    }
                    else
                    {
                        valueStr += "'" + value + "',";
                    }

                    continue; 
                }

                if (count == 63 || count == 64 || count == 74 || count == 79 || count == 84 || count == 89 || count == 91)
                {

                    if (value.Length >= 11)
                    {
                        valueStr += "'" + value.Substring(0, 10) + "',";
                    }
                    else
                    {
                        valueStr += "'" + value + "',";
                    }
                    continue;
                }
                valueStr += "'" + value + "',"; 
            }

            valueStr += "'" + FilterCorporationFromFilePath(filePath) + "',";
            return valueStr; 
        }

        //生成重组后承运人信息
        private string GenerateCarriernameunionFromCarriername(string carriername)
        {
            if(carriername == "国航" || carriername == "深航")
                return "国深航";
            
            if(carriername == "上航" || carriername == "东航")
                return "东上航";

            return carriername;
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

        private int DeleteAgtCodeWithZero(string agtCode)
        {
            return Convert.ToInt32(agtCode);
        }

        private string InsertWithSfIncome(string[] values, string valueStr)
        {

            valueStr += "'" + values[10] + "'," + //航班分类flttype：对应航班分类flttype
                        "'" + values[0] + "'," + //航班日期fltdate：对应飞行日期fltdate
                        "'" + values[39] + "'," + //执行单位company：对应执行单位company
                        "'" + values[11] + "'," + //航班号fltno：对应航班号fltno
                        "'" + values[34] + "'," + //起飞时间flttime：对应起飞时间flttime
                        "'" + values[12] + "'," + //航段segment：对应航段segment
                        "'" + values[13] + "'," + //航段片区segmentarea：对应航段片区segmentarea
                        "'" + values[14] + "'," + //航段性质segmenttype：对应航段性质segmenttype
                        "'" + values[15] + "'," + //航段中文segmentname：对应航段中文segmentname
                        "'" + values[20] + "'," + //航线分类linetype：对应航线性质linetype
                        "'" + values[21] + "'," + //航线中文linename：对应航线中文linename
                        "'" + values[16] + "'," + //航线三字码linecode:对应航线line
                        "'" + values[35] + "'," + //包机标志charterflag：对应包机标志charterflag
                        "'" + values[36] + "'," + //共享标志shareflag：对应共享标志shareflag
                        "'" + values[37] + "'," + //加班标志overtimeflag：对应加班标志overtimeflag
                        "'" + values[38] + "'," + //补贴标记moneyflag：对应补贴标记moneyflag
                        "'" + values[33] + "'," + //机型pmmodel：对应机型pmmodel
                        "'" + values[40] + "'," + //班次航班fltnum：对应班次航班fltnum
                        "'" + values[41] + "'," + //班次num：对应班次num
                        "'" + values[42] + "'," + //班次航节legnum：对应班次航节legnum
                        "'" + values[51] + "'," + //旅客人数passenger：对应登机数快报boarding 联程各航段求和
                        "'" + values[53] + "'," + //客公里kegongli：对应客公里快报kegongli 联程各航段求和
                        "'" + values[66] + "'," + //座公里zuogongli：对应座公里航节zuogonglileg 联程各航段求和
                        "'" + values[62] + "'," + //客行收入pincome：对应收入快报income 联程各航段求和
                        "'" + values[91] + "'," + //燃油附加费收入oil：对应燃油附加费oil 联程各航段求和
                        "'" + (Convert.ToDecimal(values[62]) + Convert.ToDecimal(values[91])) + "'," + //客行收入合计（含燃油）pincomeoil：对应客行收入（sfincome） + 燃油附加费收入（sfincome）入
                        "'" + values[61] + "'," +   //全票收入ticketincome：对应全票价收入Y舱全票价fullpricey 联程各航段求和
                        "'" + values[90] + "',"; //航空保险费insurance :对应航空保险费 insurance

            return valueStr;
        }

        private bool FilterLine(string carrier, string line, string shareFlag, string company)
        {
            if (carrier == "CZ" && (line.IndexOf("WUH") >= 0 || line.IndexOf("YIH") >= 0 || line.IndexOf("ENH") >= 0 || line.IndexOf("XFN") >= 0) && !(shareFlag == "1" && company == ""))
                return true;
            return false;
        }

        private string FilterLineTypes(string lineTypes)
        {
            if (lineTypes.Contains("国际"))
                return "国际";
            if (lineTypes.Contains("地区"))
                return "地区";

            return "国内";
        }

        private string GetLineValueFromCharterFlagAndOvertimeFlag(string charterFlag, string overtimeFlag)
        {
            switch (charterFlag + overtimeFlag)
            { 
                case "00":
                    return "正班";
                case "10":
                    return "包机";
                case "01":
                    return "加班";
            }
            return charterFlag + overtimeFlag;
        }


        //cargoincome
        private string InsertWithCargoIncome(TableName table, string[] values, string dateTime, string type)
        {
            var commandText = "insert into " + Enum.GetName(typeof(TableName), table) + " (" + FilterEscape(ConfigurationManager.AppSettings[Enum.GetName(typeof(TableName), table)].ToString()) + ") ";
            var valueStr = " values(";
            var count = 0;

            foreach (string value in values)
            {
                count++;
                if (count == 1) //第一列的执行单位不需要
                    continue;
                if (count == 10) //由于存在最后一列数据无法读出的情况，在这里舍弃最后一列，而采取手动计算合计的方式；
                    continue;

                valueStr += "'" + value + "',";
            }

            valueStr += "'" + (Convert.ToDecimal(values[7]) + Convert.ToDecimal(values[8])) + "',";
            valueStr += "'" + dateTime + "',";//收入导出的时间
            valueStr += AddCompany(table, type) + ");";
            commandText += valueStr;
            return commandText;
        }

        //通用的数据表转化，对于没有特殊字段的表可以调用该方法
        private string InsertWithCommon(string[] values, string valueStr, TableName table, string type)
        {
            foreach (string value in values)
            {
                valueStr += "'" + value.Replace(",", "").Replace("'", "''") + "',";
            }

            if (AddCompany(table, type) != "")
            {
                valueStr += AddCompany(table, type) + ",";
            }
            return valueStr;
        }

        private string AddCompany(TableName table, string type)
        {
            if (table == TableName.et || table == TableName.et_temp || table == TableName.cargoincome || table == TableName.flightplan || table == TableName.cincome || table == TableName.pincome)
            {
                return "'" + type.ToUpper() + "'";
            }
            return "";
        }


        //private helper
        private string FilterEscape(string value)
        {
            string filter = @"[\t\n\r\s]";
            value = System.Text.RegularExpressions.Regex.Replace(value, filter, "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return value;
        }



        //以下方法暂时没有作用，未来可以视情况进行删除
        #region
        private string GenerateUpdateStr(TableName table, string[] values)
        {
            var commandText = "update " + Enum.GetName(typeof(TableName), table) + " set ";
            var columnKeys = ConfigurationManager.AppSettings[Enum.GetName(typeof(TableName), table)].ToString().Split(',');
            commandText = UpdateWithPincome(values, commandText, columnKeys);
            return commandText;
        }

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
        #endregion
    }
}