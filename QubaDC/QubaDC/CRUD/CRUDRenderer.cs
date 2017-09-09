﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public abstract class CRUDRenderer
    {
        public abstract string RenderInsert(Table insertTable, string[] columnNames, string[] valueLiterals);
        public abstract string RenderInsertSelect(Table insertTable, string[] columnnames, string select);

        public abstract string SerializeDateTime(DateTime now);
        internal abstract string SerializeString(string v);
        internal abstract string[] RenderAutoCommitZero();


        internal string Quote(string tableReference)
        {
            return String.Format("`{0}`", tableReference);
        }

        internal string RenderJoinType(JoinType join)
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

        internal abstract string RenderUpdate(Table table, string[] columnNames, string[] valueLiterals, Restriction restriction);

        internal abstract string RenderDelete(Table table, Restriction restriction);
        internal abstract string RenderRestriction(Restriction joinCondition);
        internal abstract string[] RenderLockTables(string[] locktables);
        internal abstract string[] RenderCommitAndUnlock();
        internal abstract string[] RenderRollBackAndUnlock();
        internal abstract string renderDateTime(DateTime t);
        internal abstract string GetSQLVariable(string v);
        internal abstract string RenderNowToVariable(string v);
        internal abstract string RenderTmpTableFromSelect(string tableSchema, string tableName, string select);
        internal abstract string RenderDropTempTable(Table tmpTable);

        public abstract string PrepareTable(Table insertTable);

    }
}
