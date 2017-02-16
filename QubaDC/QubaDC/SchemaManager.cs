using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public abstract class SchemaManager
    {
        public abstract String GetCreateSchemaStatement();

        public abstract SchemaInfo GetCurrentSchema();

        public abstract SchemaInfo StoreSchema(Schema schema);

        public abstract string GetInsertSchemaStatement(Schema schema, SchemaModificationOperator smo);
    }
}
