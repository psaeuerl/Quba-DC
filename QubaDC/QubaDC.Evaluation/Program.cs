using QubaDC.Evaluation.DeleteValidation;
using QubaDC.Evaluation.Logging;
using QubaDC.Evaluation.UpdateValidation;
using QubaDC.Hybrid;
using QubaDC.Integrated;
using QubaDC.Separated;
using QubaDC.SimpleSystem;
using System;
using System.Collections.Generic;

namespace QubaDC.Evaluation
{
    class Program
    {
        static void Main(string[] args)
        {

            MySQLDataConnection connection = new MySQLDataConnection()
            {
                Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                Server = "localhost",
                DataBase = "mysql"
            };
            connection.CommandTimeout = int.MaxValue;
            //Accurate information from information schema
            //https://www.percona.com/blog/2011/12/23/solving-information_schema-slowness/
            connection.ExecuteNonQuerySQL("set global innodb_stats_on_metadata=1;");
            connection.ExecuteNonQuerySQL("set global innodb_stats_persistent=0;");

            QubaDCSystem hybridSystem = new MySQLQubaDCSystem(
                    connection,
                    new HybridSMOHandler()
                    , new HybridCRUDHandler()
                    , new HybridQSSelectHandler()
                    , new HybridMySqlSMORenderer()
                    );

            QubaDCSystem simpleSystem = new MySQLQubaDCSystem(
                connection,
                new SimpleSMOHandler(),
                new SimpleSystemCRUDHandler(),
                new SimpleQSSelectHandler(),
                new SimpleMySqlSMORenderer());

            QubaDCSystem separatedSystem = new MySQLQubaDCSystem(
                connection,
                  new SeparatedSMOHandler()
                  , new SeparatedCRUDHandler()
                , new SeparatedQSSelectHandler()
                , new SeparatedMySqlSMORenderer()
                );
            QubaDCSystem integratedSystem = new MySQLQubaDCSystem(
               connection,
               new IntegratedSMOHandler()
               , new IntegratedCRUDHandler()
               , new IntegratedQSSelectHandler()
               , new IntegratedMySqlSMORenderer()
               );

            var SystemSetups = new SystemSetup[] {
                   new SystemSetup() { quba = simpleSystem, name ="Simple", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = separatedSystem, name = "Separated", PrimaryKeyColumns = new String[]{ "ID" } },
                   new SystemSetup() { quba = hybridSystem, name = "Hybrid", PrimaryKeyColumns = new String[]{ "ID" }},
                    new SystemSetup() { quba = integratedSystem, name ="Integrated", PrimaryKeyColumns = new String[]{ "ID","startts"} },
                    };
            DateTime exec = DateTime.Now;
            Output.WriteLine("Testrun @ " + exec.ToUniversalTime());

            //RunInsertTest(hybridSystem, simpleSystem, separatedSystem, integratedSystem, 1000);
            //RunUpdateWholeTable(SystemSetups, "1k",true);

            RunDeleteByIDTable(SystemSetups, "1k", true);

            // ReanalyzeSystems(SystemSetups, "100k");
            //ReanaleyzeDbs(connection, new String[] {
            //    "simple_1k",
            //"upd_SimpleReference_9cafcdd4d0d94f35a157da95a9dca4fa",
            //"Separated_1k",
            //"upd_Separated_1fb4b23ddbfe47aab8b017f960adbf04",
            //"Hybrid_1k",
            //"upd_Hybrid_d3c1810b99dc45f4a5840d85fdcddfb9",
            //"integrated_1k",
            //"upd_Integrated_3bef43ef9cfa4a2cb745a041f23be55e"
            //});


            DBCopier dbc = new DBCopier();
            //dbc.CopyTable(SystemSetups[0], "EVAL_Integrated_647d49a93aef4ceca9e2e481f2a7a86c", "integrated_1k", true);
            //dbc.CopyTable(SystemSetups[0], "EVAL_Separated_1d37edec0b754e9982fd34fff8ba84a6", "separated_1k", true);
            //dbc.CopyTable(SystemSetups[0], "EVAL_Hybrid_7de223faed4247bab1ae5a2eac824ac5", "hybrid_1k", true);
            //dbc.CopyTable(SystemSetups[0], "EVAL_SimpleReference_ae65a9f5bb074e02867e89bcfea6c5be", "simple_1k", true);

            Output.WriteLine("--- Test Finished - Press Key to End ---");
            Output.WriteLine("Testrun Finished @ " + exec.ToLongDateString());
            Console.ReadLine();
        }

        private static void ReanaleyzeDbs(MySQLDataConnection connection, string[] v)
        {
            foreach (var db in v)
            {
                TableSizeQuerier tsq = new TableSizeQuerier()
                {
                    Connection = connection
                };
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + db);
                Output.WriteLine("DBName: " + db);

                tsq.printTableStatus(db);
                Output.WriteLine("##############################################################");
            }
        }


        private static void ReanalyzeSystems(SystemSetup[] setups, String tablesuffix)
        {

            foreach (var system in setups)
            {
                TableSizeQuerier tsq = new TableSizeQuerier()
                {
                    Connection = (MySQLDataConnection)system.quba.DataConnection
                };
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String dbtoAnalyze = system.name + "_" + tablesuffix;
                dbtoAnalyze = dbtoAnalyze.Replace("SimpleReference", "simple");
                Output.WriteLine("DBName: " + dbtoAnalyze);

                tsq.printTableStatus(dbtoAnalyze);
                Output.WriteLine("##############################################################");
            }

        }

        private static void RunDeleteByIDTable(SystemSetup[] setups, String tablesuffix, Boolean addendtimestampIndexes)
        {
            DBCopier cp = new DBCopier();
            //Preparation
            foreach (var system in setups)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String baseTable = system.name + "_" + tablesuffix;
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                var con = (MySQLDataConnection)system.quba.DataConnection;
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                if (addendtimestampIndexes)
                {
                    EndtimestampIndexer endtimestampIndexer = new EndtimestampIndexer();
                    endtimestampIndexer.AddEndTimestampIndex(system);
                }

                DeleteEveryRowByIDValidationRunner runner = new DeleteEveryRowByIDValidationRunner();
                Boolean addIndex = system.name == "Hybrid" || system.name == "Separated";
                runner.run(con, system, dbName,  addIndex);

                Output.WriteLine("DBName: " + dbName);
                Output.WriteLine("##############################################################");
            }
        }

        private static void RunUpdateWholeTable(SystemSetup[] setups, String tablesuffix, Boolean addendtimestampIndexes)
        {
            DBCopier cp = new DBCopier();
            //Preparation
            foreach (var system in setups)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String baseTable = system.name + "_" + tablesuffix;
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                var con = (MySQLDataConnection)system.quba.DataConnection;
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                if (addendtimestampIndexes)
                {
                    EndtimestampIndexer endtimestampIndexer = new EndtimestampIndexer();
                    endtimestampIndexer.AddEndTimestampIndex(system);
                }

                UpdateWholeTableValidationRunner runner = new UpdateWholeTableValidationRunner();
                Boolean addIndex = system.name == "Hybrid" || system.name == "Separated";
                runner.run(con, system, dbName, 20, addIndex);

                Output.WriteLine("DBName: " + dbName);
                Output.WriteLine("##############################################################");
            }
        }

        private static void RunUpdateEveryRowBy(SystemSetup[] setups, String tablesuffix)
        {
            DBCopier cp = new DBCopier();
            //Preparation
            foreach (var system in setups)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String baseTable = system.name + "_" + tablesuffix;
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                var con = (MySQLDataConnection)system.quba.DataConnection;
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                UpdateEveryRowByCLOBValidationRunner runner = new UpdateEveryRowByCLOBValidationRunner();
                Boolean addIndex = system.name == "Hybrid" || system.name == "Separated";
                runner.run(con, system, dbName,1000, 100, addIndex);

                Output.WriteLine("DBName: " + dbName);
                Output.WriteLine("##############################################################");
            }
        }

        private static void RunUpdateSections(SystemSetup[] setups, String tablesuffix, Boolean addendtimestampIndexes)
        {
            DBCopier cp = new DBCopier();
            //Preparation
            foreach (var system in setups)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String baseTable = system.name + "_" + tablesuffix;
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                var con = (MySQLDataConnection)system.quba.DataConnection;
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                if (addendtimestampIndexes)
                {
                    EndtimestampIndexer endtimestampIndexer = new EndtimestampIndexer();
                    endtimestampIndexer.AddEndTimestampIndex(system);
                }

                UpdateBySectionValidationRunner runner = new UpdateBySectionValidationRunner();
                Boolean addIndex = system.name == "Hybrid" || system.name == "Separated";
                runner.run(con, system, dbName, 20, addIndex);

                Output.WriteLine("DBName: " + dbName);
                Output.WriteLine("##############################################################");
            }
        }

        private static void RunUpdateEveryRow(SystemSetup[] setups, String tablesuffix,int expectedRows)
        {
            DBCopier cp = new DBCopier();
            //Preparation
            foreach(var system in setups)
            {
                Output.WriteLine("##############################################################");
                Output.WriteLine("Starting test for system:" + system.name);
                String baseTable = system.name + "_" + tablesuffix;                
                String dbName = "upd_" + system.name + "_" + Guid.NewGuid().ToString().Replace("-", "");
                var con = (MySQLDataConnection)system.quba.DataConnection;
                cp.CopyTable(system, baseTable, dbName, system.name == "SimpleReference");
                con.UseDatabase(dbName);

                UpdateEveryRowValidationRunner runner = new UpdateEveryRowValidationRunner();
                Boolean addIndex = system.name == "Hybrid" || system.name == "Separated";
                runner.run(con, system, dbName, expectedRows,1000,addIndex);

                Output.WriteLine("DBName: " + dbName);
                Output.WriteLine("##############################################################");
            }
        }

        private static void RunInsertTest(QubaDCSystem hybridSystem, QubaDCSystem simpleSystem, QubaDCSystem separatedSystem, QubaDCSystem integratedSystem, int rows)
        {
            InsertPhase p1 = new InsertPhase()
            {
                Inserts = rows,
                dropDb = false
            };

            Output.WriteLine("Starting InsertTest with " + p1.Inserts + " Inserts per System");
            InsertValidationRunner s = new InsertValidationRunner()
            {
                systems = new SystemSetup[] {
                    new SystemSetup() { quba = integratedSystem, name ="Integrated", PrimaryKeyColumns = new String[]{ "ID","startts"} },
                    new SystemSetup() { quba = separatedSystem, name = "Separated", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = hybridSystem, name = "Hybrid", PrimaryKeyColumns = new String[]{ "ID" }},
                    new SystemSetup() { quba = simpleSystem, name ="SimpleReference", PrimaryKeyColumns = new String[]{ "ID" } } },
                Phase = p1
            };
            s.Run();
        }
    }
}
