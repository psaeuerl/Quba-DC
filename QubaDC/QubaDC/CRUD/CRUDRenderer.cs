using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{

    public class TableToLock
    {
        public String Name { get; set; }
        public String Alias { get; set; }
        public Boolean LockAsWrite { get; set; }
           
    }
    public abstract class CRUDRenderer
    {
        public abstract string RenderInsert(Table insertTable, string[] columnNames, string[] valueLiterals);
        public abstract string RenderInsertSelect(Table insertTable, string[] columnnames, string select);

        public abstract string SerializeDateTime(DateTime now);
        public abstract string SerializeString(string v);
        public abstract string[] RenderAutoCommitZero();


        public string Quote(string tableReference)
        {
            return String.Format("`{0}`", tableReference);
        }

        public abstract string[] RenderLockTables(string[] locktables, bool[] lockAsWrite);

        public abstract string[] RenderLockTablesAliased(TableToLock[] tablesToLock);

        public string RenderJoinType(JoinType join)
        {
            switch (join)
            {
                case JoinType.InnerJoin:
                    return "INNER JOIN";
                case JoinType.LeftJoin:
                    return "LEFT JOIN";
                case JoinType.RightJoin:
                    return "RIGHT JOIN";
                case JoinType.NoJoin:
                    return ", ";
                default:
                    throw new NotImplementedException();
            }
        }

        public abstract string RenderUpdate(Table table, string[] columnNames, string[] valueLiterals, Restriction restriction);

        public abstract string RenderDelete(Table table, Restriction restriction);
        public abstract string RenderRestriction(Restriction joinCondition);        
        public abstract string[] RenderCommitAndUnlock();
        public abstract string[] RenderRollBackAndUnlock();
        public abstract string renderDateTime(DateTime t);
        public abstract string GetSQLVariable(string v);
        public abstract string RenderNowToVariable(string v);
        public abstract string RenderTmpTableFromSelect(string tableSchema, string tableName, string select);
        public abstract string RenderDropTempTable(Table tmpTable);

        public abstract string PrepareTable(Table insertTable);

    }
}
