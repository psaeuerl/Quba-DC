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
                    new SystemSetup() { quba = simpleSystem, name ="SimpleReference", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = separatedSystem, name = "Separated", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = hybridSystem, name = "Hybrid", PrimaryKeyColumns = new String[]{ "ID" }},
                    new SystemSetup() { quba = integratedSystem, name ="Integrated", PrimaryKeyColumns = new String[]{ "ID","startts"} },
                    };
            DateTime exec = DateTime.Now;
            Output.WriteLine("Testrun @ " + exec.ToUniversalTime());

            ////RunInsertTest(hybridSystem, simpleSystem, separatedSystem, integratedSystem, 1000);
            RunUpdateEveryRowBy(SystemSetups, "1k");
            // ReanalyzeSystems(SystemSetups, "100k");
            //ReanaleyzeDbs(connection, new String[] {
            //    "simple_1k",
            //"upd_SimpleReference_414136c5a58840c7bcc2e3304c01e851",
            //"Separated_1k",
            //"upd_Separated_163bb08f8a2c4e30b2397d1a2f29f223",
            //"Hybrid_1k",
            //"upd_Hybrid_028c5ff9584b4db0ba61c03aee543a29",
            //"integrated_1k",
            //"upd_Integrated_48cdb55b3d2644ea9de858563697397a"
            //});

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
