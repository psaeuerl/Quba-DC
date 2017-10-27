using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QubaDC.Evaluation
{

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
                    var phaseResult = phase.runFor(system.quba, dbName);
                    Console.WriteLine("Results For Phase: " + phase.phaseNumber);
                    Console.WriteLine("Insert Time ms: " + phaseResult.insertSum);
                    Console.WriteLine("MAX/MIN/MEAN " + phaseResult.insertTimes.Max() + "/" + phaseResult.insertTimes.Min() + "/" + phaseResult.insertTimes.Average());
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