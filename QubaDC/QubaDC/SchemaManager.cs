using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace QubaDC
{
    public abstract class SchemaManager
    {
        public abstract String GetCreateSchemaStatement();

        public abstract SchemaInfo GetCurrentSchema();

        public abstract string GetInsertSchemaStatement(Schema schema, SchemaModificationOperator smo, bool useUpdateTimeVariable=false);

        public void StoreSchema(Schema schema,SchemaModificationOperator smo, DataConnection con, DbConnection c, bool useUpdateTimeVariable = false)
        {
            var curSchema = GetCurrentSchema();
            String stmt = GetInsertSchemaStatement(schema, smo, useUpdateTimeVariable);
            con.ExecuteInsert(stmt, c);
        }

        public abstract SchemaInfo GetCurrentSchema(DbConnection openConnection, Action<String> log);
        public abstract SchemaInfo[] GetAllSchemataOrderdByIdDescending();
        public abstract string GetInsertToGlobalUpdateTrigger();
        internal abstract string GetTableName();
        public abstract SchemaInfo GetSchemaActiveAt(DateTime dateTime);

        internal SchemaInfo GetCurrentSchema(DbConnection c)
        {
            return GetCurrentSchema(c, (x) => {; });
        }

        internal abstract string RenderEnsureSchema(SchemaInfo xy);
        internal abstract string GetStoredProcedureExistsStatement();
        internal abstract string GetCreateEnsureIDCreateProcedure();
    }
}
