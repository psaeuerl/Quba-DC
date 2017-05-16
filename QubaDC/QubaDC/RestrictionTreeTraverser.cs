using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public abstract class RestrictionTreeTraverser<T>
    {
        //public abstract T Visit<T>(Restriction r);
        internal abstract T Visit(AndRestriction andRestriction);
        internal abstract T Visit(LiteralOperand literalOperand);
        internal abstract T Visit(ColumnOperand columnOperand);
        internal abstract T Visit(DateTimeRestrictionOperand dateTimeRestrictionOperand);

        internal abstract T Visit(OperatorRestriction operatorRestriction);
        internal abstract T Visit(ValueRestrictionOperand valueRestrictionOperand);
        internal abstract T Visit(OrRestriction orRestriction);
    }
}
