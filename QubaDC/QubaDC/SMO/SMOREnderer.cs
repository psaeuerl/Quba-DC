using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public abstract class SMORenderer
    {
        public abstract String RenderCreateTable(CreateTable ct, Boolean RemoveAdditionalColumnInfos=false);

        public abstract string RenderCreateInsertTrigger(CreateTable createTable, CreateTable ctHistTable);
        internal abstract string RenderCreateDeleteTrigger(CreateTable createTable, CreateTable ctHistTable);
        internal abstract string RenderCreateUpdateTrigger(CreateTable createTable, CreateTable ctHistTable);
        internal abstract string RenderRenameTable(RenameTable renameTable);
        internal abstract string RenderDropTable(DropTable dropTable);
    }
}
