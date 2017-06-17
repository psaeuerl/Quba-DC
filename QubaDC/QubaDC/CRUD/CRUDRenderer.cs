using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public abstract class CRUDRenderer
    {
        public abstract string RenderInsert(Table insertTable, string[] columnNames, string[] valueLiterals);

        public abstract string SerializeDateTime(DateTime now);
        internal abstract string SerializeString(string v);

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
                default:
                    throw new NotImplementedException();
            }
        }

        internal abstract string RenderUpdate(Table table, string[] columnNames, string[] valueLiterals, Restriction restriction);

        internal abstract string RenderDelete(Table table, Restriction restriction);
        internal abstract string RenderRestriction(Restriction joinCondition);

    }
}
