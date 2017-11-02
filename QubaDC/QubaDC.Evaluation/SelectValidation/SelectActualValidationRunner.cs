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

namespace QubaDC.Evaluation.SelectValidation
{
    public class SelectActualValidationRunner
    {
        public void run(MySQLDataConnection con, SystemSetup system, String dbName, int iterations, int updtIterations, int maxRows, Boolean addIndexHist)
        {

            int selectRepetitions = iterations;

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

            int[] idsToQuery = Enumerable.Range(0, 50).Select(x => r.Next(maxRows)).ToArray();

            var IDRest = new Restrictions.OperatorRestriction()
            {
                LHS = new Restrictions.ValueRestrictionOperand() { Value = "ID" },
                Op = Restrictions.RestrictionOperator.IN,
                RHS = new ValueRestrictionOperand()
                {
                    Value = String.Join(", ", idsToQuery) //surrounding brackets added automatically
                }
            };

            OperatorRestriction endTSNull = new OperatorRestriction()
            {
                LHS = new ValueRestrictionOperand()
                {
                    Value = IntegratedConstants.EndTS
                },

                Op = RestrictionOperator.IS
,
                RHS = new LiteralOperand()
                {
                    Literal = "NULL"
                },
            };




            var rest = new AndRestriction()
            {
                Restrictions = new Restriction[] { IDRest, s.Restriction, system.name == "Integrated" ? endTSNull : null }
            };
            s.Restriction = rest;

            DoSelects(system, selectRepetitions, s);

            Output.WriteLine("Starting Update");
            UpdateOperation uo = new UpdateOperation()
            {
                ColumnNames = new String[] { "ValueToUpdate" },
                Restriction = new QubaDC.Restrictions.OperatorRestriction()
                {
                    LHS = new Restrictions.ValueRestrictionOperand() { Value = "ID" },
                    RHS = new ValueRestrictionOperand()
                    {
                        Value = String.Join(", ", idsToQuery) //surrounding brackets added automatically
                    },
                    Op = Restrictions.RestrictionOperator.NotIN
                },
                Table = s.FromTable,
                ValueLiterals = new String[] { "ValueToUpdate+1" },
            };
            for (int i = 0; i < updtIterations; i++)
            {
                system.quba.CRUDHandler.HandleUpdateOperation(uo);
                Output.WriteLine("Finished one Update");
            };

            DoSelects(system, selectRepetitions, s);


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


            //int cnt = 0;
            //int toTake = iterations;
            //for(int i=0;i<iterations;i++)
            //{
            //    var sectionToUpdate = r.Next(3);
            //    UpdateOperation uo = new UpdateOperation()
            //    {
            //        ColumnNames = new String[] { "ValueToUpdate" },
            //        Restriction = new QubaDC.Restrictions.OperatorRestriction()
            //        {
            //            LHS = new Restrictions.ValueRestrictionOperand() { Value = "Section" },
            //            RHS = new Restrictions.ValueRestrictionOperand() { Value = sectionToUpdate.ToString() },
            //            Op = Restrictions.RestrictionOperator.Equals
            //        },
            //        Table = s.FromTable,
            //        ValueLiterals = new String[] { "ValueToUpdate+1" },
            //    };
            //    sw.Start();
            //    system.quba.CRUDHandler.HandleUpdateOperation(uo);
            //    sw.Stop();
            //    var time = sw.ElapsedMilliseconds;
            //    updateValues.Add(time);
            //    sw.Reset();
            //    cnt++;
            //    if (cnt % (toTake / 10) == 0)
            //        Console.WriteLine(DateTime.Now.ToLongTimeString()+" updated " + cnt);
            //    Output.WriteLine(time.ToString());

            //}
            //Output.WriteLine("Insert Time ms: " + updateValues.Sum());
            //Output.WriteLine("MAX/MIN/MEAN " + updateValues.Max() + "/" + updateValues.Min() + "/" + updateValues.Average());

            //Output.WriteLine("Finished DB Values");
            //PrintDBStatistics(system, dbName);
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
