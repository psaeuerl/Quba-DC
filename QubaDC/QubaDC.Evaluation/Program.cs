using QubaDC.Evaluation.Logging;
using QubaDC.Hybrid;
using QubaDC.Integrated;
using QubaDC.Separated;
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
                new SimpleSystem.SimpleSMOHandler(),
                new SimpleSystem.SimpleSystemCRUDHandler(),
                new SimpleSystem.SimpleQSSelectHandler(),
                new SimpleSystem.SimpleMySqlSMORenderer());

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

            DateTime exec = DateTime.Now;
            Output.WriteLine("Testrun @ " + exec.ToLongDateString());

            RunInsertTest(hybridSystem, simpleSystem, separatedSystem, integratedSystem);

            Output.WriteLine("--- Test Finished - Press Key to End ---");
            Output.WriteLine("Testrun Finished @ " + exec.ToLongDateString());
            Console.ReadLine();
        }


        private static void RunInsertTest(QubaDCSystem hybridSystem, QubaDCSystem simpleSystem, QubaDCSystem separatedSystem, QubaDCSystem integratedSystem)
        {
            InsertPhase p1 = new InsertPhase()
            {
                Inserts = 100000,
                dropDb = false
            };

            Output.WriteLine("Starting InsertTest with " + p1.Inserts + " Inserts per System");
            InsertValidationRunner s = new InsertValidationRunner()
            {
                systems = new SystemSetup[] {
                    new SystemSetup() { quba = separatedSystem, name = "Separated", PrimaryKeyColumns = new String[]{ "ID" } },
                    new SystemSetup() { quba = hybridSystem, name = "Hybrid", PrimaryKeyColumns = new String[]{ "ID" }},
                    new SystemSetup() { quba = integratedSystem, name ="Integrated", PrimaryKeyColumns = new String[]{ "ID","startts"} },
                    new SystemSetup() { quba = simpleSystem, name ="SimpleReference", PrimaryKeyColumns = new String[]{ "ID" } } },
                Phase = p1
            };
            s.Run();
        }
    }
}
