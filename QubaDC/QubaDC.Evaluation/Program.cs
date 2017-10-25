using QubaDC.Hybrid;
using QubaDC.Separated;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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


            List<Phase> phases = new List<Phase>();
            Phase p1 = new Phase() {                
                phaseNumber = 1,
                Inserts = 100,
                Updates = 0,
                Deletes = 0,
                //TODO => Selects
            };
            phases.Add(p1);
            Testsetup s = new Testsetup()
            {
                systems = new SystemSetup[] { new SystemSetup() { quba = hybridSystem, name = "Hybrid" },
                                              new SystemSetup() { quba = simpleSystem, name ="SimpleReference" } },
                Phases = phases
            };

            s.Run();

        }
    }

    internal class SystemSetup
    {
        internal string name;
        internal QubaDCSystem quba;
    }

    internal class Testsetup
    {
        public Testsetup()
        {
        }

        public List<Phase> Phases { get; internal set; }
        public SystemSetup[] systems { get; internal set; }

        internal void Run()
        {
            foreach(var system in systems)
            {
                Console.WriteLine("Starting test for system:" + system.name);
                String dbName = String.Format("EVAL_{0}_{1}", system.name, Guid.NewGuid().ToString().Replace("-", ""));
                Console.WriteLine("DBName: " + dbName);
                UseDB((MySQLDataConnection)system.quba.DataConnection, dbName);

                //SETUP
                CreateTable t = EvaluationCreateTable.GetTable(dbName);
                system.quba.CreateSMOTrackingTableIfNeeded();
                system.quba.SMOHandler.HandleSMO(t);

                foreach(var phase in Phases)
                {
                    phase.runFor(system.quba, dbName);
                }


                Console.WriteLine("Finished test for system: " + system.name);
            }

            Console.ReadLine();
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
