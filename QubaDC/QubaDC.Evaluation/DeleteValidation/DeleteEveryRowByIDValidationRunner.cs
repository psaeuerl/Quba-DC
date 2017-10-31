﻿using QubaDC.CRUD;
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
    public class DeleteEveryRowByIDValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String dbName,  Boolean addIndexHist)
        {
            if (addIndexHist)
            {
                Output.WriteLine("Adding Primary key at Hist Table on Columns ID + Starttimestamp");
                String stmt = "ALTER TABLE {0}.datatable_1 ADD PRIMARY KEY(ID, startts)";
                String stmtSQL = String.Format(stmt, dbName);
                con.ExecuteNonQuerySQL(stmtSQL);
            }
            Random r = new Random(20);
            Output.WriteLine("Initial DB Values");
            PrintDBStatistics(system, dbName);

            SelectOperation s = SelectOperationGenerator.GetBasicSelectAll(dbName);
            String select = system.quba.CRUDHandler.RenderSelectOperation(s);
            DataTable t = con.ExecuteQuery(select);
            List<int> allIds = t.Select().Select(x => int.Parse(x.ItemArray[1].ToString())).ToList();
            int[] itIteration = new int[allIds.Count];
            for(int i=0;i<itIteration.Length; i++)
            {
                int nextIndex = r.Next(allIds.Count);
                itIteration[i] = allIds[nextIndex];
                allIds.RemoveAt(nextIndex);
            }

            List<long> updateValues = new List<long>();
            Stopwatch sw = new Stopwatch();
            int cnt = 0;
            foreach (int id in itIteration)
            {

                DeleteOperation uo = new DeleteOperation()
                {
                    Restriction = new QubaDC.Restrictions.OperatorRestriction()
                    {
                        LHS = new Restrictions.ValueRestrictionOperand() { Value = "ID" },
                        RHS = new Restrictions.ValueRestrictionOperand() { Value = id.ToString() },
                        Op = Restrictions.RestrictionOperator.Equals
                    },
                    Table = s.FromTable
                };
                sw.Start();
                system.quba.CRUDHandler.HandleDeletOperation(uo);
                sw.Stop();
                var time = sw.ElapsedMilliseconds;
                updateValues.Add(time);
                sw.Reset();
                cnt++;
                if (cnt % (itIteration.Length / 10) == 0)
                    Console.WriteLine(DateTime.Now.ToLongTimeString()+" deleted " + cnt);
               // Output.WriteLine(time.ToString());
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
