using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CTTable
{
    public static class IEnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Split<T>(this IEnumerable<T> list, int partsSize)
        {
            return list.Select((item, index) => new { index, item })
                       .GroupBy(x => x.index / partsSize)
                       .Select(x => x.Select(y => y.item));
        }
    }

    public class SqlTextQuery
    {
        private readonly string _sqlDatabaseConnectionString;

        public SqlTextQuery(string sqlDatabaseConnectionString)
        {
            _sqlDatabaseConnectionString = sqlDatabaseConnectionString;
        }

        public IEnumerable<Dictionary<string, object>> PerformQuery(string query)
        {
            var command = new SqlCommand(query);
            //CommandType.Text;

            IEnumerable<Dictionary<string, object>> result = null;
            using (var sqlConnection = new SqlConnection(_sqlDatabaseConnectionString))
            {
                sqlConnection.Open();

                command.Connection = sqlConnection;
                using (SqlDataReader r = command.ExecuteReader())
                {
                    result = Serialize(r);
                }
                sqlConnection.Close();
            }
            return result;
        }

        private IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            while (reader.Read())
            {
                var row = new Dictionary<string, object>();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row.Add(reader.GetName(i), reader.GetValue(i));
                }
                results.Add(row);
            }
            return results;
        }
        public DataTable GetDataTable(string sql)

        {
            using (var cn = new SqlConnection(_sqlDatabaseConnectionString))
            {
                cn.Open();
                using (SqlDataAdapter da = new SqlDataAdapter(sql, cn))
                {
                    da.SelectCommand.CommandTimeout = 120;                
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    return ds.Tables[0];                
                }
            }
        }
    }

    class Program
    {
        static ConsoleColor defaultcolor = Console.ForegroundColor;
        static int SQLBatchSize = 1;
        static int ExecutionControl = 1;
        static int ExecutionControlSleepMs = 1000;
        static string sqlDatabaseConnectionString = string.Empty;
        static string serviceBusConnectionString = string.Empty;
        static string hubName = String.Empty;
        static string eventHubKey = string.Empty;
        static string eventKeyName = string.Empty;
        static string eventHubNameSpace = string.Empty;
        static string sqlServerName = string.Empty;
        static string databaseName = string.Empty;

        static void Main(string[] args)
        {
            if(args.Length ==0)
            {
                Console.WriteLine("You must specify args: /b /e /s /d /v /h /k /r /n");
            }
            foreach (string s in args)
            {
                switch (s.Substring(1, 1).ToLower())
                {
                    case "b":
                        if(!int.TryParse(s.Substring(2).ToString(),out SQLBatchSize))
                        {
                            cout(ConsoleColor.Red, "Invalid batch Size");
                            return;
                        }
                        break;

                    case "e":
                        if (!int.TryParse(s.Substring(2).ToString(), out ExecutionControl))
                        {
                            cout(ConsoleColor.Red, "Invalid Execution Control Value");
                            return;
                        }
                        break;

                    case "s":
                        if (!int.TryParse(s.Substring(2).ToString(), out ExecutionControlSleepMs))
                        {
                            cout(ConsoleColor.Red, "Invalid Execution Control Sleep Value");
                            return;
                        }
                        break;

                    case "d":
                        //SQLServer Database
                        string d = s.Substring(2).ToString().ToLower();
                        if (d == string.Empty)
                        {
                            cout(ConsoleColor.Red, "Database Name cannot be empty");
                            return;
                        }
                        databaseName = d;
                        break;

                    case "v":
                        //SQLServer Connection String
                        string v = s.Substring(2).ToString().ToLower();
                        if (v == string.Empty)
                        {
                            cout(ConsoleColor.Red, "SQLServer cannot be empty");
                            return;
                        }
                        sqlServerName = v;
                        break;

                    case "h":
                        //Event hub name space
                        eventHubNameSpace = s.Substring(2).ToString().ToLower();
                        if (eventHubNameSpace == string.Empty)
                        {
                            cout(ConsoleColor.Red, "SQLServer cannot be empty");
                            return;
                        }
                        break;

                    case "k":
                        //Event hub access key
                        eventHubKey = s.Substring(2).ToString();
                        if (eventHubKey == string.Empty)
                        {
                            cout(ConsoleColor.Red, "SQLServer cannot be empty");
                            return;
                        }
                        break;

                    case "r":
                        //Event hub access name
                        eventKeyName = s.Substring(2).ToString();
                        if (eventKeyName == string.Empty)
                        {
                            cout(ConsoleColor.Red, "SQLServer cannot be empty");
                            return;
                        }
                        break;

                    case "n":
                        hubName = s.Substring(2).ToString().ToLower();
                        if (hubName == string.Empty)
                        {
                            cout(ConsoleColor.Red, "SQLServer cannot be empty");
                            return;
                        }
                        break;
                }
            }
            if (databaseName== string.Empty|sqlServerName ==string.Empty|eventHubNameSpace ==string.Empty|eventHubKey==string.Empty|eventKeyName==string.Empty|hubName==string.Empty)
            {
                cout(ConsoleColor.Red, "The following args are required: /d /v /h /k /r /n");
                return;
            }

            sqlDatabaseConnectionString = String.Format(@"Server={0};Database={1};Integrated Security=True;Connection Timeout=30;",sqlServerName,databaseName);
            serviceBusConnectionString = String.Format(@"Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={2};SharedAccessKey={1}", eventHubNameSpace, eventHubKey, eventKeyName);

            // VARIABLES
            string updateOffsetQuery;
            string insertDataQuery;
            string selectOffsetQuery;
            string selectCDCTableQuery;
            string offsetString;
            string selectDataQuery;
            string nextOffset;

            // ESTABLISH SQL & HUB CONNECTIONS
            SqlTextQuery queryPerformer = new SqlTextQuery(sqlDatabaseConnectionString);

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(serviceBusConnectionString)

            {

                EntityPath = hubName

            };

            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            selectCDCTableQuery = "select name from sys.tables where name like '%_CT'";
            DataTable cdcQueryResult = queryPerformer.GetDataTable(selectCDCTableQuery);


            do
            {
                //For each CT Table in CTTables 
                foreach (DataRow ctTable in cdcQueryResult.Rows)
                {
                    // GET CURRENT OFFSET FOR SQL TABLE
                    string Coretable_CT = "cdc."+ctTable["name"].ToString();
                    string Coretable = ctTable["name"].ToString().Replace("_", ".").Substring(0,ctTable["name"].ToString().Length - 3);
                    
                    selectOffsetQuery = String.Format(@"select convert(nvarchar(100), LastMaxVal, 1) as stringLastMaxVal from StreamData_Tracker.dbo.StreamData_TableOffset where TableName = '{0}';",Coretable);
                    Dictionary<string, object> offsetQueryResult = queryPerformer.PerformQuery(selectOffsetQuery).FirstOrDefault();
                    DataTable offsetTable = queryPerformer.GetDataTable(selectOffsetQuery);

                    if (offsetTable.Rows.Count >0)
                    {
                        //Table exists get the Core Table name
                        offsetString = offsetQueryResult.Values.First().ToString();
                    }
                    else
                    {
                        //IF it doesnt exist enter starting value
                        insertDataQuery = string.Format(@"INSERT INTO [StreamData_Tracker].[dbo].[StreamData_TableOffset] ([TableName],[LastMaxVal],[LastUpdateDateTime],[LastCheckedDateTime]) VALUES ('{0}',0x00000000000000000000, '1900-01-01 00:00:00', '1900-01-01 00:00:00');",Coretable);
                        //insertDataQuery = "INSERT INTO [StreamData_Tracker].[dbo].[StreamData_TableOffset] ([TableName],[LastMaxVal],[LastUpdateDateTime],[LastCheckedDateTime]) VALUES ('" + Coretable+ "',0x00000000000000000000, '1900-01-01 00:00:00', '1900-01-01 00:00:00')";
                        offsetString = "0x00000000000000000000";
                        DataTable InsertMissingData = queryPerformer.GetDataTable(insertDataQuery);
                    }

                    // GET ROWS FROM SQL CDC CHANGE TRACKING TABLE GREATER THAN THE OFFSET
                    selectDataQuery = String.Format(@"select '{0}' as CTTable_TableName, ROW_NUMBER() OVER (ORDER BY [__$start_lsn]) as CTTable_RowNbr, convert(nvarchar(100), __$start_lsn, 1) as CTTable_$start_lsn_string, convert(nvarchar(100), __$seqval, 1) as CTTable_$seqval_string, convert(nvarchar(100), __$update_mask, 1) as CTTable_$update_mask_string, * from {1} where __$start_lsn > {2} order by __$start_lsn;",Coretable,Coretable_CT,offsetString);
                    //selectDataQuery = "select '" + Coretable + "' as CTTable_TableName, ROW_NUMBER() OVER (ORDER BY [__$start_lsn]) as CTTable_RowNbr, convert(nvarchar(100), __$start_lsn, 1) as CTTable_$start_lsn_string, convert(nvarchar(100), __$seqval, 1) as CTTable_$seqval_string, convert(nvarchar(100), __$update_mask, 1) as CTTable_$update_mask_string, * from " +Coretable_CT + " where __$start_lsn > " + offsetString + " order by __$start_lsn";
                    DataTable CTTableResult = queryPerformer.GetDataTable(selectOffsetQuery);
                    IEnumerable<Dictionary<string, object>> resultCollection = queryPerformer.PerformQuery(selectDataQuery);

                    // IF THERE ARE NEW ROWS TO SEND...
                    if (CTTableResult.Rows.Count >0 & resultCollection.Any())
                    {
                        IEnumerable<Dictionary<string, object>> orderedByColumnName = resultCollection.OrderBy(r => r["CTTable_RowNbr"]);

                        // GROUP ROWS TO SEND INTO A MESSAGE BATCH
                        foreach (var resultGroup in orderedByColumnName.Split(SQLBatchSize))
                        {
                            // SEND BATCH ROWS TO EVENT HUB AS JSON MESSAGE
                            SendRowsToEventHub(eventHubClient, resultGroup).Wait();

                            // UPDATE CURRENT VALUE IN SQL TABLE OFFSET
                            nextOffset = resultGroup.Max(r => r["CTTable_$start_lsn_string"].ToString());
                            updateOffsetQuery = String.Format(@"update StreamData_Tracker.dbo.StreamData_TableOffset set LastMaxVal = convert(binary(10), '{0}', 1), LastUpdateDateTime = getdate() where TableName = '{1}';",nextOffset,Coretable);
                           //updateOffsetQuery = "update StreamData_Tracker.dbo.StreamData_TableOffset set LastMaxVal = convert(binary(10), '" + nextOffset + "', 1), LastUpdateDateTime = getdate() where TableName = '" + Coretable + "'";
                            queryPerformer.PerformQuery(updateOffsetQuery);
                        }
                    }

                    // UPDATE OFFSET LAST CHECKED DATE
                    updateOffsetQuery = string.Format(@"update StreamData_Tracker.dbo.StreamData_TableOffset set LastCheckedDateTime = getdate() where TableName = '{0}';", Coretable);
                    //updateOffsetQuery = "update StreamData_Tracker.dbo.StreamData_TableOffset set LastCheckedDateTime = getdate() where TableName = '" + Coretable + "'";
                    queryPerformer.PerformQuery(updateOffsetQuery);

                    // WAIT BEFORE ITERATING LOOP
                    Thread.Sleep(ExecutionControlSleepMs);
                }
            }
            while (ExecutionControl == 1); // LOOP IF RUN CONTINUOUS ENABLED
        }

        private static async Task SendRowsToEventHub(EventHubClient eventHubClient, IEnumerable<object> rows)
        {
            var memoryStream = new MemoryStream();

            using (var sw = new StreamWriter(memoryStream, new UTF8Encoding(false), 1024, leaveOpen: true))
            {
                string serialized = JsonConvert.SerializeObject(rows);
                sw.Write(serialized);
                sw.Flush();
            }

            Debug.Assert(memoryStream.Position > 0, "memoryStream.Position > 0");

            memoryStream.Position = 0;
            EventData eventData = new EventData(memoryStream.ToArray());

            await eventHubClient.SendAsync(eventData);
        }
        // output to console
        static void cout(ConsoleColor c, string message)
        {
            Console.ForegroundColor = c;
            Console.WriteLine(message.ToString());
            Console.ForegroundColor = defaultcolor;
        }
    }
}
