using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public class MySQLRestrictionVisitor : RestrictionTreeTraverser<String>
    {

        private MySQLCrudRenderer CRUDRender { get; set; }
        public MySQLRestrictionVisitor(MySQLCrudRenderer renderer)
        {
            this.CRUDRender = renderer;
        }

        internal override string Visit(OrRestriction orRestriction)
        {
            var children = orRestriction.Restrictions.Select(x => x.Accept<String>(this)).ToArray();
            String joined = String.Join(" OR ", children);
            String result = "(" + joined + ")";
            return result;

        }

        internal override string Visit(ColumnOperand columnOperand)
        {
            string column = CRUDRender.RenderColumn(columnOperand.Column);
            return column;
        }

        internal override string Visit(ValueRestrictionOperand valueRestrictionOperand)
        {
            throw new NotImplementedException();
        }

        internal override string Visit(DateTimeRestrictionOperand dateTimeRestrictionOperand)
        {            
            return CRUDRender.SerializeDateTime(dateTimeRestrictionOperand.Value);
        }

        internal override string Visit(LiteralOperand literalOperand)
        {
            return literalOperand.Literal;
        }

        internal override string Visit(OperatorRestriction operatorRestriction)
        {
            String LHS = operatorRestriction.LHS.Accept<String>(this);
            String op = Render(operatorRestriction.Op);
            String RHS = operatorRestriction.RHS.Accept<String>(this);

            String result = "(" + String.Join(" ", LHS, op, RHS) + ")";
            return result;
        }

        private string Render(RestrictionOperator op)
        {
            switch (op)
            {
                case RestrictionOperator.Equals:
                    return "=";
                case RestrictionOperator.LT:
                    return "<";
                case RestrictionOperator.LET:
                    return "<=";
                case RestrictionOperator.GT:
                    return ">";
                case RestrictionOperator.GET:
                    return ">=";
                case RestrictionOperator.IS:
                    return "IS";
                default:
                    throw new NotImplementedException(op.ToString());
            }
        }

        internal override string Visit(AndRestriction andRestriction)
        {
            var children = andRestriction.Restrictions.Select(x => x.Accept<String>(this)).ToArray();
            String joined = String.Join(" AND ", children);
            String result = "(" + joined + ")";
            return result;
        }

        internal override string Visit(RestrictionRestrictionOperand restrictionRestrictionOperand)
        {
            return restrictionRestrictionOperand.Restriciton.Accept<String>(this);
        }
    }
}
