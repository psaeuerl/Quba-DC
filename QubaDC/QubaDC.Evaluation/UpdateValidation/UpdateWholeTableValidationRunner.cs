using QubaDC.CRUD;
using QubaDC.Evaluation.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation.UpdateValidation
{
    public class UpdateWholeTableValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String dbName, int iterations, Boolean addIndexHist)
        {
            if (addIndexHist)
            {
                Output.WriteLine("Adding Primary key at Hist Table on Columns ID + Starttimestamp");
                String stmt = "ALTER TABLE {0}.datatable_1 ADD PRIMARY KEY(ID, startts)";
                String stmtSQL = String.Format(stmt, dbName);
                con.ExecuteNonQuerySQL(stmtSQL);
            }
            Output.WriteLine("Initial DB Values");
            PrintDBStatistics(system, dbName);

            SelectOperation s = SelectOperationGenerator.GetBasicSelectAll(dbName);
            String select = system.quba.CRUDHandler.RenderSelectOperation(s);
            //DataTable t = con.ExecuteQuery(select);
            //if (t.Rows.Count != expectedRows)
            //    throw new InvalidOperationException("Didnt get expected rows");
            //List<string> allClobs = t.Select().Select(x => x.ItemArray[4].ToString()).ToList();
            //string[] itIteration = new string[allClobs.Count];
            //for(int i=0;i<itIteration.Length;i++)
            //{
            //    int nextIndex = r.Next(allClobs.Count);
            //    itIteration[i] = allClobs[nextIndex];
            //    allClobs.RemoveAt(nextIndex);
            //}

            List<long> updateValues = new List<long>();
            Stopwatch sw = new Stopwatch();
            int cnt = 0;
            int toTake = iterations;
            for(int i=0;i<iterations;i++)
            {
                UpdateOperation uo = new UpdateOperation()
                {
                    ColumnNames = new String[] { "ValueToUpdate" },
                    Restriction = null,
                    Table = s.FromTable,
                    ValueLiterals = new String[] { "ValueToUpdate+1" },
                };
                sw.Start();
                system.quba.CRUDHandler.HandleUpdateOperation(uo);
                sw.Stop();
                var time = sw.ElapsedMilliseconds;
                updateValues.Add(time);
                sw.Reset();
                cnt++;
                if (cnt % (toTake / 10) == 0)
                    Console.WriteLine(DateTime.Now.ToLongTimeString()+" updated " + cnt);
                Output.WriteLine(time.ToString());

            }
            Output.WriteLine("Insert Time ms: " + updateValues.Sum());
            Output.WriteLine("MAX/MIN/MEAN " + updateValues.Max() + "/" + updateValues.Min() + "/" + updateValues.Average());

            Output.WriteLine("Finished DB Values");
            PrintDBStatistics(system, dbName);
        }

        private static void PrintDBStatistics(SystemSetup system, string dbName)
        {
            var allTables = system.quba.DataConnection.GetAllTables();
            new TableFlusher().flushTables(system.quba.DataConnection,
                allTables.Select(x => x.Schema + "." + x.Name),
                dbName,
                 new Dictionary<string, ulong>()
                );
            TableSizeQuerier q = new TableSizeQuerier()
            {
                Connection = (MySQLDataConnection)system.quba.DataConnection
            };
            q.printTableStatus(dbName);
            DBStatusQuerier qdb = new DBStatusQuerier()
            {
                Connection = (MySQLDataConnection)system.quba.DataConnection
            };
            qdb.getDBSize(dbName);
        }
    }
}
