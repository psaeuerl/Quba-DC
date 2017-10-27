using QubaDC.Hybrid;
using QubaDC.Integrated;
using QubaDC.Separated;
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

            List<Phase> phases = new List<Phase>();
            Phase p1 = new Phase() {                
                phaseNumber = 1,
                Inserts = 100,
                Updates = 0,
                Deletes = 0,
                 DoDelets = false,
                  DoUPdates = false
                //TODO => Selects
            };
            phases.Add(p1);
            Testsetup s = new Testsetup()
            {
                systems = new SystemSetup[] {
                    new SystemSetup() { quba = separatedSystem, name = "Separated" },
                    new SystemSetup() { quba = hybridSystem, name = "Hybrid" },   
                    new SystemSetup() { quba = integratedSystem, name ="Integrated" },
                    new SystemSetup() { quba = simpleSystem, name ="SimpleReference" } },
                Phases = phases
            };

            s.Run();

        }
    }
}
