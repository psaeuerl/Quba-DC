using QubaDC.CRUD;
using QubaDC.Evaluation.Logging;
using QubaDC.Integrated;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation.QSValidation
{
    public class QSDeleteAllSectionValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String dbName, int numberQueries,  Boolean addIndexHist)
        {

            int selectRepetitions = numberQueries;

            if (addIndexHist)
            {
                Output.WriteLine("Adding Primary key at Hist Table on Columns ID + Starttimestamp");
                String stmt = "ALTER TABLE {0}.datatable_1 ADD PRIMARY KEY(ID, startts)";
                String stmtSQL = String.Format(stmt, dbName);
                con.ExecuteNonQuerySQL(stmtSQL);
            }
            Random r = new Random(20);
            Output.WriteLine("Initial DB Values");
            //PrintDBStatistics(system, dbName);

            SelectOperation s = SelectOperationGenerator.GetBasicSelectAll(dbName);


            var baseRest = s.Restriction;







            List<long> firstSelectValues = new List<long>();
            List<QueryStoreSelectResult> selectResults = new List<QueryStoreSelectResult>();
            Stopwatch sw = new Stopwatch();
            String select = system.quba.CRUDHandler.RenderSelectOperation(s);

            PrintDBStatistics(system, dbName);


            long selectRenderTime = sw.ElapsedMilliseconds;
            int[] ids = new int[] { 1, 2, 0, 6, 5, 7, 8, 9, 3, 4 };
            for (int i = 0; i < selectRepetitions; i++)
            {
                var IDRest = new Restrictions.OperatorRestriction()
                {
                    LHS = new Restrictions.ValueRestrictionOperand() { Value = "SECTION" },
                    Op = Restrictions.RestrictionOperator.Equals,
                    RHS = new LiteralOperand()
                    {
                        Literal = ids[i].ToString()
                    }
                };
                var rest = new AndRestriction()
                {
                    Restrictions = new Restriction[] { IDRest, baseRest }
                };
                s.Restriction = rest;

                System.Threading.Thread.Sleep(500);
                String query = system.quba.CRUDHandler.RenderSelectOperation(s);
                query = query.Replace("SELECT", "SELECT SQL_NO_CACHE");
                if (system.name == "Simple")
                {
                    sw.Start();
                    system.quba.DataConnection.ExecuteQuery(query);
                    sw.Stop();
                }else
                {
                    sw.Start();
                    var result = system.quba.QueryStore.ExecuteSelect(s);
                    sw.Stop();
                    selectResults.Add(result);
                }

                var time = sw.ElapsedMilliseconds;
                firstSelectValues.Add(time);
                sw.Reset();
                Output.WriteLine(time.ToString());

            }
            Output.WriteLine("Select render time: " + selectRenderTime);
            Output.WriteLine("Select Time ms: " + firstSelectValues.Sum());
            Output.WriteLine("MAX/MIN/MEAN " + firstSelectValues.Max() + "/" + firstSelectValues.Min() + "/" + firstSelectValues.Average());

            if (system.name == "Simple")
            {
                Output.WriteLine("Simple finished!");
                return;
            }

            Output.WriteLine("Deleting all rows");
            DeleteOperation uo = new DeleteOperation()
            {
                Restriction = null,
                Table = s.FromTable
            };
            system.quba.CRUDHandler.HandleDeletOperation(uo);

            List<long> reselectValues = new List<long>();
            Output.WriteLine("REEXECUTION");
            foreach(var result in selectResults)
            {
                sw.Start();
                var reexecResult = system.quba.QueryStore.ReExecuteSelect(result.GUID);
                sw.Stop();
                var time = sw.ElapsedMilliseconds;
                reselectValues.Add(time);
                sw.Reset();
                Output.WriteLine(time.ToString());
            }

            Output.WriteLine("Select Time ms: " + reselectValues.Sum());
            Output.WriteLine("MAX/MIN/MEAN " + reselectValues.Max() + "/" + reselectValues.Min() + "/" + reselectValues.Average());

            
            PrintDBStatistics(system, dbName);
        }

        private static void DoSelects(SystemSetup system, int selectRepetitions, SelectOperation s)
        {
            List<long> firstSelectValues = new List<long>();
            Stopwatch sw = new Stopwatch();
            String select = system.quba.CRUDHandler.RenderSelectOperation(s);
            Output.WriteLine("Initial Select Time");

            sw.Start();
            String query = system.quba.CRUDHandler.RenderSelectOperation(s);
            //query = query.Replace("SELECT", "SELECT SQL_NO_CACHE");
            sw.Stop();
            long selectRenderTime = sw.ElapsedMilliseconds;

            String flushTables = String.Format(@"FLUSH TABLES {0}.{1}", s.FromTable.TableSchema,s.FromTable.TableName);            
            for (int i = 0; i < selectRepetitions; i++)
            {

                system.quba.DataConnection.ExecuteNonQuerySQL(flushTables);
                System.Threading.Thread.Sleep(500);
                sw.Start();
                DataTable result = system.quba.DataConnection.ExecuteQuery(query);
                sw.Stop();
                var time = sw.ElapsedMilliseconds;
                firstSelectValues.Add(time);
                sw.Reset();
                Output.WriteLine(time.ToString());
                
            }

            Output.WriteLine("Select: " + query);
            Output.WriteLine("Select render time: " + selectRenderTime);
            Output.WriteLine("Select Time ms: " + firstSelectValues.Sum());
            Output.WriteLine("MAX/MIN/MEAN " + firstSelectValues.Max() + "/" + firstSelectValues.Min() + "/" + firstSelectValues.Average());
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
