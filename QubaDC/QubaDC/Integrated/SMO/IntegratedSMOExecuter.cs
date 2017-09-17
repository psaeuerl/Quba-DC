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

    }
    public class IntegratedSMOExecuter
    {



        public static void Execute(SMORenderer SMORenderer
            , DataConnection DataConnection
            //, string[] PreLockingStatements
            //, string[] AfterLockingStatemnts
            //, string[] tablesToLock
            //, Boolean[] lockAsWrite
            , SchemaManager schemaManager
            , SchemaModificationOperator op
            , Func<SchemaInfo, UpdateSchema> RenderStatements
            , Action<String> logStatements
            )
        {

            CRUDRenderer crudRenderer = SMORenderer.CRUDRenderer;
            String[] beforeLockStatements = new String[]
        {
                        "SET autocommit=0;",
                        "SELECT GET_LOCK('SMO UPDATES',10);",
                        "SET @updateTime = NOW(3); "
        };

            String[] cleanup = new String[]
            {
                "COMMIT;",
                "SELECT RELEASE_LOCK('SMO UPDATES');"
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
                ExecuteStatements(DataConnection, con, updateSchema.UpdateStatements, logStatements);
                String storeSchema =  schemaManager.GetInsertSchemaStatement(updateSchema.newSchema, op, true);

                ExecuteStatements(DataConnection, con, new String[] { storeSchema }, logStatements);

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
