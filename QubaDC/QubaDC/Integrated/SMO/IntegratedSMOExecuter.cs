using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.SMO
{
    public class UpdateSchema
    {
        public String[] UpdateStatements { get; set; }
        public Schema newSchema { get; set; }

        public Table[] MetaTablesToLock { get; set; }

        public Table[] TablesToUnlock { get; set; }

    }
    public class IntegratedSMOExecuter
    {



        public static void Execute(SMORenderer SMORenderer
            , DataConnection DataConnection
            //, string[] AfterLockingStatemnts
            //, string[] tablesToLock
            //, Boolean[] lockAsWrite
            , SchemaManager schemaManager
            , SchemaModificationOperator op
            , Func<SchemaInfo, UpdateSchema> RenderStatements
            , Action<String> logStatements
            , TableMetadataManager metaManager
            )
        {

            CRUDRenderer crudRenderer = SMORenderer.CRUDRenderer;
            String[] beforeLockStatements = new String[]
        {
                        "SET autocommit=0;",
                        "SELECT GET_LOCK('SMO UPDATES',10);"                       
        };

            String[] afterTablesClearedStmt = new string[]
            {
                 "SET @updateTime = NOW(3); "
            };

            String[] cleanup = new String[]
            {
                "COMMIT;",
                "SELECT RELEASE_LOCK('SMO UPDATES');"
            };

            String[] unlockCommit = new String[]
            {
                "COMMIT;",
"UNLOCK TABLES; "
            };
            DataConnection.AquiereOpenConnection(con =>
            {
                ExecuteStatements(DataConnection, con, beforeLockStatements, logStatements);

                SchemaInfo currentSchemaInfo = schemaManager.GetCurrentSchema(con);
                Schema currentSchema = currentSchemaInfo.Schema;
                if (currentSchemaInfo.ID == null)
                {
                    currentSchema = new Schema();
                    currentSchemaInfo.ID = 0;
                }

                UpdateSchema updateSchema = RenderStatements(currentSchemaInfo);

                //Lock Tables
                String[] tablesToLock = updateSchema.MetaTablesToLock.Select(x => metaManager.GetMetaTableFor(x.TableSchema, x.TableName))
                                                                    .Select(x => SMORenderer.CRUDRenderer.PrepareTable(x))
                                                                    .ToArray();
                if (tablesToLock.Length > 0)
                {
                    Boolean[] lockAsWrite = tablesToLock.Select(x => true).ToArray();
                    String[] lockTables = crudRenderer.RenderLockTables(tablesToLock, lockAsWrite);
                    ExecuteStatements(DataConnection, con, lockTables, logStatements);

                    //SetCanBeQueried
                    String[] canBeQueriedFalse = updateSchema.MetaTablesToLock.Select(x => metaManager.SetCanBeQueriedFalse(x)).ToArray();
                    ExecuteStatements(DataConnection, con, canBeQueriedFalse, logStatements);

                    //Unlock tables
                    ExecuteStatements(DataConnection, con, unlockCommit, logStatements);
                } else if(typeof(CreateTable) != op.GetType())
                {
                    throw new InvalidOperationException("Only Create Table needs no tablesToLock");
                }

                ExecuteStatements(DataConnection, con, afterTablesClearedStmt, logStatements);

                ExecuteStatements(DataConnection, con, updateSchema.UpdateStatements, logStatements);
                String storeSchema =  schemaManager.GetInsertSchemaStatement(updateSchema.newSchema, op, true);                

                ExecuteStatements(DataConnection, con, new String[] { storeSchema }, logStatements);

                //release Can Be Queried
                String[] setCanBeQueriedTrue = updateSchema.TablesToUnlock.Select(x => metaManager.SetCanBeQueriedTrue(x)).ToArray();
                if (setCanBeQueriedTrue.Length > 0)
                {
                    ExecuteStatements(DataConnection, con, setCanBeQueriedTrue, logStatements);
                } else if(op.GetType() != typeof(CreateTable))
                {
                    throw new InvalidOperationException("Only create table needs no unlock");
                }


                ExecuteStatements(DataConnection, con, cleanup, logStatements);
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
