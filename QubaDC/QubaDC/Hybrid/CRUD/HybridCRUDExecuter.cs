using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid.CRUD
{
    public class HybridCRUDExecuter
    {
        public static void DefLog(String s )
        {
            System.Diagnostics.Debug.WriteLine(s);
        }
        public static void ExecuteStatementsOnLockedTables(Func<String[]> RenderStatements, String[] locktables,Boolean[]lockAsWrite, DataConnection DataConnection, CRUDRenderer crudRenderer, SchemaManager schemaManager, SchemaInfo expectedSchema, Table changingTable, TableMetadataManager metaManager,
            Action<String> logStatements
        )
        {

            DataConnection.AquiereOpenConnection(con =>
            {
                String[] beforeLock = crudRenderer.RenderAutoCommitZero();
                ExecuteStatements(DataConnection, con, beforeLock,logStatements);

                String[] lockTable = crudRenderer.RenderLockTables(locktables, lockAsWrite);
                ExecuteStatements(DataConnection, con, lockTable, logStatements);

                //Ensure Hist has not changed
                SchemaInfo schemaDuringInsert = schemaManager.GetCurrentSchema(con);
                TableSchema histDuringInsert = schemaDuringInsert.Schema.FindHistTable(changingTable);
                TableSchema histBeforeInsert = expectedSchema.Schema.FindHistTable(changingTable);
                logStatements("-- C# ensuring hist table has not changed");
                if (histDuringInsert.Name != histBeforeInsert.Name)
                {
                    throw new InvalidOperationException("Hist during Insert is not Hist during before lock, expected: " + histBeforeInsert.Name + " got: " + histDuringInsert.Name);
                }

                //Ensure Can be queried
                Boolean canBeQueried =  metaManager.GetCanBeQueriedFor(changingTable,con,logStatements);
                logStatements("-- C# Code checking canBeQueried");
                if(!canBeQueried)
                {
                    throw new InvalidOperationException("Table cannot be queried currently as SMO is in effect");
                }
                String[] insertStatements = RenderStatements();
                ExecuteStatements(DataConnection, con, insertStatements, logStatements);

                String[] commitUnlock = crudRenderer.RenderCommitAndUnlock();
                ExecuteStatements(DataConnection, con, commitUnlock, logStatements);
            });
        }

        private static void ExecuteStatements(DataConnection DataConnection, System.Data.Common.DbConnection con, string[] beforeLock, Action<String> logStatements)
        {
            foreach (var stmt in beforeLock)
            {
                logStatements(stmt);
                DataConnection.ExecuteNonQuerySQL(stmt, con);
            }
        }
    }
}
