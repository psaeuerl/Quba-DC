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
    public class UpdateEveryRowValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String dbName, int expectedRows)
        {
            Random r = new Random(20);
            Output.WriteLine("Initial Vlaues");
            PrintDBStatistics(system, dbName);

            SelectOperation s = SelectOperationGenerator.GetBasicSelectAll(dbName);
            String select = system.quba.CRUDHandler.RenderSelectOperation(s);
            DataTable t = con.ExecuteQuery(select);
            if (t.Rows.Count != expectedRows)
                throw new InvalidOperationException("Didnt get expected rows");
            List<int> allIds = t.Select().Select(x => int.Parse(x.ItemArray[1].ToString())).ToList();
            int[] itIteration = new int[allIds.Count];
            for(int i=0;i<itIteration.Length;i++)
            {
                int nextIndex = r.Next(allIds.Count);
                itIteration[i] = allIds[nextIndex];
                allIds.Remove(nextIndex);
            }

            List<long> updateValues = new List<long>();
            Stopwatch sw = new Stopwatch();
            int cnt = 0;
            int toTake = 20;
            foreach (int id in itIteration.Take(toTake))
            {

                UpdateOperation uo = new UpdateOperation()
                {
                    ColumnNames = new String[] { "ValueToUpdate" },
                    Restriction = new QubaDC.Restrictions.OperatorRestriction()
                    {
                        LHS = new Restrictions.ValueRestrictionOperand() { Value = "ID" },
                        RHS = new Restrictions.ValueRestrictionOperand() { Value = id.ToString() },
                        Op = Restrictions.RestrictionOperator.Equals
                    },
                    Table = s.FromTable,
                    ValueLiterals = new String[] { "ValueToUpdate+1" },
                };
                sw.Start();
                system.quba.CRUDHandler.HandleUpdateOperation(uo);
                sw.Stop();
                updateValues.Add(sw.ElapsedMilliseconds);
                sw.Reset();
                cnt++;
                if (cnt % (toTake / 10) == 0)
                    Console.WriteLine("updated " + cnt);

            }
            Output.WriteLine("Insert Time ms: " + updateValues.Sum());
            Output.WriteLine("MAX/MIN/MEAN " + updateValues.Max() + "/" + updateValues.Min() + "/" + updateValues.Average());

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
