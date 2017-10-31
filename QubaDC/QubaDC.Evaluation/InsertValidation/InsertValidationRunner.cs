using QubaDC.Evaluation.Logging;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QubaDC.Evaluation
{

    internal class InsertValidationRunner
    {
        public InsertValidationRunner()
        {
        }

        public InsertPhase Phase { get; internal set; }
        public SystemSetup[] systems { get; internal set; }
        public int Sections { get; internal set; }

        internal void Run()
        {

            foreach (var system in systems)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String dbName = String.Format("EVAL_{0}_{1}", system.name, Guid.NewGuid().ToString().Replace("-", ""));
                Output.WriteLine("DBName: " + dbName);
                UseDB((MySQLDataConnection)system.quba.DataConnection, dbName);

                //SETUP
                system.quba.Init();
                CreateTable t = EvaluationCreateTable.GetTable(dbName);                
                system.quba.SMOHandler.HandleSMO(t);

                //ADDing Primary Key 
                Console.Write("Adding PrimaryKey on Columns: " + String.Join(",",system.PrimaryKeyColumns));
                String stmt = String.Format("ALTER TABLE {0}.{1} ADD PRIMARY KEY({2});", dbName, t.TableName, String.Join(",", system.PrimaryKeyColumns));
                system.quba.DataConnection.ExecuteNonQuerySQL(stmt);

                var phaseResult = Phase.runFor(system.quba, dbName, Sections);
                Output.WriteLine("Insert Time ms: " + phaseResult.insertSum);
                Output.WriteLine("MAX/MIN/MEAN " + phaseResult.insertTimes.Max() + "/" + phaseResult.insertTimes.Min() + "/" + phaseResult.insertTimes.Average());

                var expectedValues = new Dictionary<string, ulong>();
                expectedValues.Add(EvaluationCreateTable.GetTable(dbName).TableName, Convert.ToUInt64(Phase.Inserts));

                var allTables = system.quba.DataConnection.GetAllTables();
                new TableFlusher().flushTables( system.quba.DataConnection,
                    allTables.Select(x=>x.Schema+"."+x.Name),
                    dbName,
                    expectedValues     
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


                if (Phase.dropDb)
                    system.quba.DataConnection.ExecuteNonQuerySQL("DROP DATABASE " + dbName);
                Output.WriteLine("Finished test for system: " + system.name);
                Output.WriteLine("##############################################################");
            }
        }

        private void UseDB(MySQLDataConnection dataConnection, string dbName)
        {
            try
            {
                dataConnection.ExecuteNonQuerySQL("DROP DATABASE " + dbName);
            }
            catch (InvalidOperationException ex)
            {
                var e = ex.InnerException.Message;
                if (!(e.Contains("Can't drop database '") && e.Contains("'; database doesn't exist")))
                {
                    throw ex;
                };
            }
            dataConnection.ExecuteNonQuerySQL("CREATE DATABASE " + dbName);
            dataConnection.UseDatabase(dbName);
        }
    }
}