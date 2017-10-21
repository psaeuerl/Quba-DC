using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid
{
    public class HybridQSSelectExecuter
    {
        public static void DefLog(String s )
        {
            ;
        }
        public long ExecuteStatementsOnLockedTables(
            Func<DataTable,DataTable, String> RenderInsert,      
            Func<SchemaInfo,DateTime,String[]> RenderSelectStatements,
           TableToLock[]tablesToLock,
            DataConnection DataConnection, 
            CRUDRenderer crudRenderer, 
            SchemaManager schemaManager, 
            //SchemaInfo expectedSchema, 
            Table[] changingTables, 
            TableMetadataManager metaManager,
            Action<String> logStatements
        )
        {

            String[] setUpdateTime = new string[]
            {
                             "SET @updateTime = NOW(3); "
            };

            String[] unlockTables = new String[]
                {
                "UNLOCK TABLES; "
                };

            String[] commit = new String[]
            {
                "COMMIT;"
            };
            //logStatements = (s) => { System.Diagnostics.Debug.WriteLine(s); };
            long result = 0;
            DataConnection.AquiereOpenConnection(con =>
            {
                String[] beforeLock = crudRenderer.RenderAutoCommitZero();
                ExecuteStatements(DataConnection, con, beforeLock,logStatements);


                TableToLock[] actualTablesToLock = tablesToLock.Union(new TableToLock[] { new TableToLock { Name = schemaManager.GetTableName(), Alias = null, LockAsWrite = false } })
                .ToArray();
                String[] lockTable = crudRenderer.RenderLockTablesAliased(actualTablesToLock);
                ExecuteStatements(DataConnection, con, lockTable, logStatements);

                //Check that all can be queried
                foreach (var changingTable in changingTables)
                {
                    Boolean canBeQueried = metaManager.GetCanBeQueriedFor(changingTable, con, logStatements);
                    logStatements("-- C# Code checking canBeQueried, throwing exception if one is false");
                    if (!canBeQueried)
                    {
                        throw new InvalidOperationException("Table cannot be queried currently as SMO is in effect");
                    }
                }





                //SetUpdate Time
                ExecuteStatements(DataConnection, con, setUpdateTime, logStatements);

                String selectUpdateTime = ("SELECT @updateTime");
                logStatements("SELECT @updateTime");
                DataTable updateTimeTable = DataConnection.ExecuteQuery(selectUpdateTime, con);
                DateTime updateTime = DateTime.Parse(updateTimeTable.Select()[0].Field<String>(0));

                SchemaInfo currentSchema = schemaManager.GetCurrentSchema(con, logStatements);
              
                String[] selects = RenderSelectStatements(currentSchema, updateTime);
                logStatements(selects[0]);
                DataTable hashTable = DataConnection.ExecuteQuery(selects[0], con);

                logStatements(selects[1]);
                DataTable actualTable = DataConnection.ExecuteQuery(selects[1], con);

                //Unlock Tables
                ExecuteStatements(DataConnection, con, unlockTables, logStatements);

                String insert = RenderInsert(hashTable, actualTable);
                logStatements(insert);
                long? id = DataConnection.ExecuteInsert(insert, con);
                result= id.Value;

                ExecuteStatements(DataConnection, con, commit, logStatements);

            });
            return result;
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
