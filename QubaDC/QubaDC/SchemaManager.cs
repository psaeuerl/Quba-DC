using QubaDC.DatabaseObjects;
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

        public abstract Schema GetCurrentSchema();

        public abstract void StoreSchema(Schema schema);

    }
}
