using QubaDC.CRUD;
using QubaDC.Evaluation.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation.DeleteValidation
{
    public class DeleteEveryRowValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String tablesuffix, int times, Boolean addIndexHist)
        {
            DBCopier cp = new DBCopier();
            Dictionary<int, List<long>> valuesPerRun = new Dictionary<int, List<long>>();
            for (int i = 0; i < times; i++)
            {

                //Copy Database
                String baseTable = system.name + "_" + tablesuffix;
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                //Test Database
                EndtimestampIndexer endtimestampIndexer = new EndtimestampIndexer();
                endtimestampIndexer.AddEndTimestampIndex(system);


                if (addIndexHist)
                {
                    Output.WriteLine("Adding Primary key at Hist Table on Columns ID + Starttimestamp");
                    String stmt = "ALTER TABLE {0}.datatable_1 ADD PRIMARY KEY(ID, startts)";
                    String stmtSQL = String.Format(stmt, dbName);
                    con.ExecuteNonQuerySQL(stmtSQL);
                }


                SelectOperation s = SelectOperationGenerator.GetBasicSelectAll(dbName);
                String select = system.quba.CRUDHandler.RenderSelectOperation(s);
                DataTable t = con.ExecuteQuery(select);
                //List<int> allIds = t.Select().Select(x => int.Parse(x.ItemArray[1].ToString())).ToList();
                //int[] itIteration = new int[allIds.Count];
                //for(int i=0;i<itIteration.Length; i++)
                //{
                //    int nextIndex = r.Next(allIds.Count);
                //    itIteration[i] = allIds[nextIndex];
                //    allIds.RemoveAt(nextIndex);
                //}


                Stopwatch sw = new Stopwatch();
                int cnt = 0;


                    DeleteOperation uo = new DeleteOperation()
                    {
                        Restriction = null,
                        Table = s.FromTable
                    };
                    sw.Start();
                    system.quba.CRUDHandler.HandleDeletOperation(uo);
                    sw.Stop();
                int key = 0;
                    if (!valuesPerRun.ContainsKey(key))
                        valuesPerRun.Add(key, new List<long>());
                    var time = sw.ElapsedMilliseconds;
                    valuesPerRun[key].Add(time);
                    sw.Reset();
                    cnt++;
                
                Output.WriteLine("finished run: "+i);
            }

            foreach (var key in valuesPerRun.Keys)
            {
                var value = valuesPerRun[key];
                long max = value.Max();
                long min = value.Min();
                double avg = value.Average();
                Output.WriteLine(String.Format("RUN {0}, Min/Max/Avg: {1}\t{2}\t{3}", value, min, max, avg));

            }

            //Output.WriteLine("Insert Time ms: " + updateValues.Sum());
            //Output.WriteLine("MAX/MIN/MEAN " + updateValues.Max() + "/" + updateValues.Min() + "/" + updateValues.Average());

            Output.WriteLine("Finished DB Values");

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
