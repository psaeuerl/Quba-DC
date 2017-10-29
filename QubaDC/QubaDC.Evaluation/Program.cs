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
                    new SystemSetup() { quba = integratedSystem, name ="Integrated", PrimaryKeyColumns = new String[]{ "ID","startts"} },
                    new SystemSetup() { quba = separatedSystem, name = "Separated", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = hybridSystem, name = "Hybrid", PrimaryKeyColumns = new String[]{ "ID" }},
                    };
            DateTime exec = DateTime.Now;
            Output.WriteLine("Testrun @ " + exec.ToUniversalTime());

            //RunInsertTest(hybridSystem, simpleSystem, separatedSystem, integratedSystem, 1000);
            RunUpdateEveryRow(SystemSetups, "1k",1000);

            Output.WriteLine("--- Test Finished - Press Key to End ---");
            Output.WriteLine("Testrun Finished @ " + exec.ToLongDateString());
            Console.ReadLine();
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
                runner.run(con, system, dbName, expectedRows);

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
