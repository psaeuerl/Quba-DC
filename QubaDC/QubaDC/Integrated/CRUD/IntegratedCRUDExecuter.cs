using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    public class IntegratedCRUDExecuter
    {
        public static void ExecuteStatementsOnLockedTables(Func<String[]> RenderStatements, String[] locktables,Boolean[]lockAsWrite, DataConnection DataConnection, CRUDRenderer crudRenderer, SchemaManager schemaManager, SchemaInfo expectedSchema, Table changingTable, TableLastUpdateManager metaManager)
        {

            DataConnection.AquiereOpenConnection(con =>
            {
                String[] beforeLock = crudRenderer.RenderAutoCommitZero();
                ExecuteStatements(DataConnection, con, beforeLock);

                String[] lockTable = crudRenderer.RenderLockTables(locktables, lockAsWrite);
                ExecuteStatements(DataConnection, con, lockTable);

                //Ensure Hist has not changed
                SchemaInfo schemaDuringInsert = schemaManager.GetCurrentSchema(con);
                TableSchema histDuringInsert = schemaDuringInsert.Schema.FindHistTable(changingTable);
                TableSchema histBeforeInsert = expectedSchema.Schema.FindHistTable(changingTable);
                if(histDuringInsert.Name != histBeforeInsert.Name)
                {
                    throw new InvalidOperationException("Hist during Insert is not Hist during before lock, expected: " + histBeforeInsert.Name + " got: " + histDuringInsert.Name);
                }

                //Ensure Can be queried
                Boolean canBeQueried =  metaManager.GetCanBeQueriedFor(changingTable,con);
                if(!canBeQueried)
                {
                    throw new InvalidOperationException("Table cannot be queried currently as SMO is in effect");
                }
                String[] insertStatements = RenderStatements();
                ExecuteStatements(DataConnection, con, insertStatements);

                String[] commitUnlock = crudRenderer.RenderCommitAndUnlock();
                ExecuteStatements(DataConnection, con, commitUnlock);
            });
        }

        private static void ExecuteStatements(DataConnection DataConnection, System.Data.Common.DbConnection con, string[] beforeLock)
        {
            foreach (var stmt in beforeLock)
                DataConnection.ExecuteNonQuerySQL(stmt, con);
        }
    }
}
