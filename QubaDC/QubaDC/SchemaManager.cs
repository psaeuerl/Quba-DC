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

        public abstract string GetInsertSchemaStatement(Schema schema, SchemaModificationOperator smo);

        public abstract SchemaInfo GetCurrentSchema(DbConnection openConnection);
        public abstract SchemaInfo[] GetAllSchemataOrderdByIdDescending();
        public abstract string GetInsertToGlobalUpdateTrigger();
    }
}
